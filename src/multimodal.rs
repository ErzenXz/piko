use crate::{computer::IMAGE_LIMIT, workspace::truncate};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

pub struct Output {
    pub text: String,
    pub images: Vec<Value>,
}

pub fn normalize(value: &Value, limit: usize) -> Output {
    let Some(blocks) = value["content"].as_array() else {
        return Output {
            text: truncate(&value.to_string(), limit),
            images: Vec::new(),
        };
    };
    let mut output = Output {
        text: String::new(),
        images: Vec::new(),
    };
    if value["isError"] == true {
        output.text.push_str("Tool reported an error.\n");
    }
    for block in blocks {
        match block["type"].as_str() {
            Some("text") => output.text.push_str(&truncate(
                block["text"].as_str().unwrap_or(""),
                limit.saturating_sub(output.text.len()),
            )),
            Some("image") => {
                let mime = block["mimeType"].as_str().unwrap_or("");
                let data = block["data"].as_str().unwrap_or("");
                let valid = data.len() <= IMAGE_LIMIT.div_ceil(3) * 4
                    && STANDARD.decode(data).ok().is_some_and(|bytes| {
                        bytes.len() <= IMAGE_LIMIT
                            && image_dimensions(mime, &bytes).is_some_and(|(w, h)| {
                                w > 0
                                    && h > 0
                                    && w <= 4096
                                    && h <= 4096
                                    && u64::from(w) * u64::from(h) <= 8_000_000
                            })
                    });
                if valid && output.images.is_empty() {
                    output.images.push(json!({"type":"input_image","image_url":format!("data:{mime};base64,{data}")}));
                } else {
                    output.text.push_str("\n[Image omitted: unsupported format, invalid data, or image budget exceeded]\n");
                }
            }
            Some("resource") => {
                output.text.push_str(&truncate(
                    block["resource"]["text"]
                        .as_str()
                        .unwrap_or("[Non-text resource omitted]"),
                    limit.saturating_sub(output.text.len()),
                ));
            }
            Some("resource_link") => output.text.push_str(&truncate(
                &format!(
                    "\nResource link (not fetched): {} {}",
                    block["name"], block["uri"]
                ),
                limit.saturating_sub(output.text.len()),
            )),
            _ => output.text.push_str("\n[Unsupported content omitted]\n"),
        }
        if output.text.len() > limit {
            output.text = truncate(&output.text, limit);
        }
    }
    if let Some(structured) = value.get("structuredContent") {
        output.text.push_str(&truncate(
            &format!("\nStructured content: {structured}"),
            limit.saturating_sub(output.text.len()),
        ));
    }
    output.text = truncate(&output.text, limit);
    output
}

pub fn image_dimensions(mime: &str, bytes: &[u8]) -> Option<(u32, u32)> {
    match mime {
        "image/png"
            if bytes.len() >= 33
                && bytes.starts_with(b"\x89PNG\r\n\x1a\n")
                && bytes[8..16] == [0, 0, 0, 13, b'I', b'H', b'D', b'R'] =>
        {
            Some((
                u32::from_be_bytes(bytes[16..20].try_into().ok()?),
                u32::from_be_bytes(bytes[20..24].try_into().ok()?),
            ))
        }
        "image/jpeg" if bytes.starts_with(&[0xff, 0xd8]) => {
            let mut at = 2;
            while at + 4 <= bytes.len() {
                if bytes[at] != 0xff {
                    return None;
                }
                at += 1;
                while at < bytes.len() && bytes[at] == 0xff {
                    at += 1;
                }
                let marker = *bytes.get(at)?;
                at += 1;
                if marker == 0xd9 || marker == 0xda {
                    return None;
                }
                if marker == 0x01 || (0xd0..=0xd7).contains(&marker) {
                    continue;
                }
                let length =
                    usize::from(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?));
                if length < 2 || at + length > bytes.len() {
                    return None;
                }
                if matches!(marker,0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
                    if length < 8 {
                        return None;
                    }
                    return Some((
                        u32::from(u16::from_be_bytes(bytes[at + 5..at + 7].try_into().ok()?)),
                        u32::from(u16::from_be_bytes(bytes[at + 3..at + 5].try_into().ok()?)),
                    ));
                }
                at += length;
            }
            None
        }
        _ => None,
    }
}

pub fn trim_images(items: &mut [Value], budget: usize) {
    let mut used = 0;
    for item in items.iter_mut().rev() {
        trim_image_item(item, budget, &mut used);
    }
}

pub fn trim_context_images(
    prefix: &mut [std::borrow::Cow<'_, Value>],
    active: &mut [Value],
    budget: usize,
) -> bool {
    let mut used = 0;
    for item in active.iter_mut().rev() {
        trim_image_item(item, budget, &mut used);
    }
    let mut changed = false;
    for item in prefix.iter_mut().rev() {
        let mut next = used;
        let needs_copy = item["type"] == "function_call_output"
            && item["output"].as_array().is_some_and(|blocks| {
                blocks
                    .iter()
                    .rev()
                    .filter(|block| block["type"] == "input_image")
                    .any(|block| {
                        let size = block["image_url"].as_str().map_or(0, str::len);
                        if size > budget.saturating_sub(next) {
                            true
                        } else {
                            next += size;
                            false
                        }
                    })
            });
        if needs_copy {
            changed = true;
            trim_image_item(item.to_mut(), budget, &mut used);
        } else {
            used = next;
        }
    }
    changed
}

fn trim_image_item(item: &mut Value, budget: usize, used: &mut usize) {
    if item["type"] != "function_call_output" {
        return;
    }
    if let Some(blocks) = item["output"].as_array_mut() {
        for block in blocks
            .iter_mut()
            .rev()
            .filter(|block| block["type"] == "input_image")
        {
            let size = block["image_url"].as_str().map_or(0, str::len);
            if size <= budget.saturating_sub(*used) {
                *used += size;
            } else {
                *block = json!({"type":"input_text","text":"[Older screenshot omitted to bound context; take a fresh screenshot if needed]"});
            }
        }
    }
}

pub fn text(output: &Value) -> String {
    output.as_str().map(str::to_owned).unwrap_or_else(|| {
        output
            .as_array()
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default()
    })
}

pub fn acp_content(output: &Value) -> Vec<Value> {
    let mut content = vec![json!({"type":"content","content":{"type":"text","text":text(output)}})];
    if let Some(blocks) = output.as_array() {
        for block in blocks {
            if block["type"] != "input_image" {
                continue;
            }
            if let Some((prefix, data)) = block["image_url"]
                .as_str()
                .and_then(|url| url.split_once(";base64,"))
                && let Some(mime) = prefix.strip_prefix("data:")
                && ["image/png", "image/jpeg"].contains(&mime)
            {
                content.push(json!({"type":"content","content":{"type":"image","mimeType":mime,"data":data}}));
            }
        }
    }
    content
}
