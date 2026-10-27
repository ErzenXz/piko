use piko::{
    config::Config,
    multimodal::trim_context_images,
    provider::Input,
    session::{Session, Store, Turn},
    workspace::Workspace,
};
use serde_json::{Value, json};
use std::{borrow::Cow, sync::Arc};

fn turn(items: Vec<Value>) -> Turn {
    Turn {
        items: Arc::new(items),
    }
}

fn image_result(id: &str, images: &[&str]) -> Value {
    json!({
        "type":"function_call_output", "call_id":id,
        "output":images.iter().map(|url| json!({"type":"input_image","image_url":url})).collect::<Vec<_>>()
    })
}

#[tokio::test]
async fn projection_borrows_large_history_and_omits_whole_turns() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = Workspace::new(directory.path()).await.unwrap();
    let mut session = Session::new(&workspace, &Config::default());
    session.turns.push_back(turn(vec![
        json!({"role":"user","content":"x".repeat(128 * 1024)}),
    ]));
    session.turns.push_back(turn(vec![
        json!({"role":"user","content":"newer"}),
        json!({"role":"assistant","content":"answer"}),
    ]));

    let (all, omitted) = session.project(1024 * 1024);
    assert_eq!(omitted, 0);
    assert_eq!(all.len(), 3);
    assert!(all.iter().all(|value| matches!(value, Cow::Borrowed(_))));
    assert!(std::ptr::eq(all[0].as_ref(), &session.turns[0].items[0]));
    assert_eq!(
        all[0]["content"].as_str().unwrap().as_ptr(),
        session.turns[0].items[0]["content"]
            .as_str()
            .unwrap()
            .as_ptr()
    );
    assert!(std::ptr::eq(all[2].as_ref(), &session.turns[1].items[1]));

    let (recent, omitted) = session.project(1024);
    assert_eq!(omitted, 1);
    assert_eq!(recent.len(), 2);
    assert!(std::ptr::eq(recent[0].as_ref(), &session.turns[1].items[0]));
    assert_eq!(session.turns.len(), 2);
    assert_eq!(
        session.turns[0].items[0]["content"].as_str().unwrap().len(),
        128 * 1024
    );
    let (empty, omitted) = session.project(1);
    assert!(empty.is_empty());
    assert_eq!(omitted, 2);
}

#[tokio::test]
async fn session_snapshots_share_turns_and_copy_only_explicitly_mutated_items() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = Workspace::new(directory.path()).await.unwrap();
    let mut session = Session::new(&workspace, &Config::default());
    session
        .turns
        .push_back(turn(vec![json!({"role":"user","content":"saved"})]));
    session.turns.push_back(turn(vec![
        json!({"role":"assistant","content":"unchanged"}),
    ]));
    let snapshot = session.clone();
    assert!(Arc::ptr_eq(
        &snapshot.turns[0].items,
        &session.turns[0].items
    ));
    assert!(Arc::ptr_eq(
        &snapshot.turns[1].items,
        &session.turns[1].items
    ));
    Arc::make_mut(&mut session.turns[0].items)[0]["content"] = json!("edited copy");
    assert!(!Arc::ptr_eq(
        &snapshot.turns[0].items,
        &session.turns[0].items
    ));
    assert!(Arc::ptr_eq(
        &snapshot.turns[1].items,
        &session.turns[1].items
    ));
    assert_eq!(snapshot.turns[0].items[0]["content"], "saved");
    assert_eq!(session.turns[0].items[0]["content"], "edited copy");
    let encoded = serde_json::to_value(&snapshot).unwrap();
    assert!(encoded["turns"][0]["items"].is_array());
    assert_eq!(encoded["turns"][0]["items"][0]["content"], "saved");
}

#[test]
fn wire_input_keeps_borrowed_history_owned_notice_and_active_items_in_order() {
    let saved = [
        json!({"role":"user","content":"history"}),
        json!({"type":"function_call","call_id":"call-1","name":"read_file","arguments":"{}"}),
    ];
    let notice = json!({"role":"developer","content":"older turns omitted"});
    let prefix = vec![
        Cow::Owned(notice.clone()),
        Cow::Borrowed(&saved[0]),
        Cow::Borrowed(&saved[1]),
    ];
    let active = vec![
        json!({"type":"function_call_output","call_id":"call-1","output":"result"}),
        json!({"role":"user","content":"current"}),
    ];
    let input = Input {
        prefix: &prefix,
        active: &active,
    };
    let iterated: Vec<_> = input.iter().collect();
    assert!(std::ptr::eq(iterated[1], &saved[0]));
    assert!(std::ptr::eq(iterated[2], &saved[1]));
    assert!(std::ptr::eq(iterated[3], &active[0]));
    assert_eq!(
        serde_json::to_value(input).unwrap(),
        json!([notice, saved[0], saved[1], active[0], active[1]])
    );
    assert_eq!(
        serde_json::to_value(Input::plain(&active)).unwrap(),
        json!(active)
    );
    assert_eq!(serde_json::to_value(Input::plain(&[])).unwrap(), json!([]));
}

#[test]
fn image_budget_copies_only_overflowing_prefix_items_and_preserves_durable_images() {
    let original = vec![
        image_result("old", &["12345678", "12"]),
        json!({"role":"user","content":"keep borrowing this"}),
        image_result("recent", &["1234"]),
    ];
    let before = original.clone();
    let mut prefix: Vec<_> = original.iter().map(Cow::Borrowed).collect();
    let mut active = vec![image_result("current", &["123"])];
    trim_context_images(&mut prefix, &mut active, 10);
    assert!(matches!(prefix[0], Cow::Owned(_)));
    assert!(matches!(prefix[1], Cow::Borrowed(_)));
    assert!(matches!(prefix[2], Cow::Borrowed(_)));
    assert!(std::ptr::eq(prefix[1].as_ref(), &original[1]));
    assert!(std::ptr::eq(prefix[2].as_ref(), &original[2]));
    assert_eq!(prefix[0]["output"][0]["type"], "input_text");
    assert_eq!(prefix[0]["output"][1]["image_url"], "12");
    assert_eq!(prefix[2]["output"][0]["image_url"], "1234");
    assert_eq!(active[0]["output"][0]["image_url"], "123");
    assert_eq!(original, before);
    // Rechecking the same window must not clone still-borrowed or rewrite kept images.
    let encoded = serde_json::to_value(&prefix).unwrap();
    trim_context_images(&mut prefix, &mut active, 10);
    assert_eq!(serde_json::to_value(&prefix).unwrap(), encoded);
    assert!(matches!(prefix[1], Cow::Borrowed(_)));
    assert!(matches!(prefix[2], Cow::Borrowed(_)));
}

#[test]
fn image_budget_applies_to_active_items_before_borrowed_history() {
    let saved = image_result("saved", &["1234"]);
    let mut prefix = vec![Cow::Borrowed(&saved)];
    let mut active = vec![image_result("active", &["12345", "1"])];
    trim_context_images(&mut prefix, &mut active, 2);
    assert_eq!(active[0]["output"][0]["type"], "input_text");
    assert_eq!(active[0]["output"][1]["image_url"], "1");
    assert_eq!(prefix[0]["output"][0]["type"], "input_text");
    assert_eq!(saved["output"][0]["image_url"], "1234");
}

#[tokio::test]
async fn arc_snapshot_save_and_cold_restore_preserve_turn_arrays_and_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = Workspace::new(directory.path()).await.unwrap();
    let store = Store::new(directory.path().join("sessions")).await.unwrap();
    let mut session = Session::new(&workspace, &Config::default());
    session
        .turns
        .push_back(turn(vec![json!({"role":"user","content":"durable"})]));
    store.save(&session).await.unwrap();
    let snapshot = session.clone();
    session.release_history(0);
    assert!(session.turns.is_empty());
    assert_eq!(snapshot.turns[0].items[0]["content"], "durable");
    session.model = "new-model".into();
    session.mode = "plan".into();
    store.hydrate(&mut session, &workspace).await.unwrap();
    assert_eq!(session.turns[0].items[0]["content"], "durable");
    assert_eq!(session.model, "new-model");
    assert_eq!(session.mode, "plan");
    store.save(&session).await.unwrap();
    let loaded = store.load(session.id, &workspace).await.unwrap();
    assert_eq!(loaded.model, "new-model");
    assert_eq!(loaded.mode, "plan");
    assert_eq!(loaded.turns[0].items[0]["content"], "durable");
    let path = directory
        .path()
        .join("sessions")
        .join(format!("{}.json", session.id));
    let persisted: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(persisted["turns"][0]["items"].is_array());
    assert_eq!(persisted["turns"][0]["items"][0]["content"], "durable");
}
