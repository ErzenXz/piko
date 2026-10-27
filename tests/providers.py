"""Provider policies and recovery: local HTTP only, no API credentials or paid calls."""
import asyncio
import json
import time
import unittest

from stdio import Harness, call, final, response
from reliability import ScriptedHarness


def chat(*events, done=False):
    return ''.join('data: ' + json.dumps(event) + '\n\n' for event in events) + ('data: [DONE]\n\n' if done else '')


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_modern_cache_fast_and_actual_tier_preserve_stable_prefixes(self):
        async def model(body):
            events = final('ok')
            return events.replace('"output": [', '"service_tier": "default", "output": [')
        async with Harness(model, extra='provider_profile = "openai"\nservice_tier = "fast"\n') as h:
            await h.initialize()
            session = await h.session()
            for prompt in ['first', 'second']:
                self.assertNotIn('error', await h.prompt(session, prompt))
            first, second = h.requests
            self.assertEqual(first['prompt_cache_options'], {'mode': 'implicit', 'ttl': '30m'})
            self.assertEqual(first['service_tier'], 'fast')
            self.assertNotIn('prompt_cache_retention', first)
            for field in ['instructions', 'tools', 'prompt_cache_key']:
                self.assertEqual(first[field], second[field])
            self.assertEqual(first['input'][0], second['input'][0])
            usage = [u['params']['usage'] for u in h.updates if u.get('method') == '_harness/usage']
            self.assertEqual(usage[-1]['serviceTier'], 'default', 'Report actual tier, not requested Fast')

    async def test_child_cache_key_is_compact_distinct_and_inherits_fast(self):
        async def model(body):
            if body['input'][-1].get('type') == 'function_call_output':
                return final()
            if body['input'][0].get('content', '').strip() == 'root':
                return response([call('spawn_agent', {'prompt': 'child', 'role': 'explore', 'background': False})])
            return final()
        async with Harness(model, extra='provider_profile = "openai"\nservice_tier = "fast"\n') as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session(), 'root'))
            self.assertEqual(len(h.requests), 3)
            keys = [body['prompt_cache_key'] for body in h.requests]
            self.assertEqual(keys[0], keys[2])
            self.assertNotEqual(keys[0], keys[1])
            self.assertTrue(all(len(key) <= 64 for key in keys))
            self.assertTrue(all(body['service_tier'] == 'fast' for body in h.requests))

    async def test_legacy_cache_retention_is_normalized(self):
        async with Harness(lambda_body, extra='provider_profile = "openai"\nmodel = "gpt-5.4"\nprompt_cache_retention = "in-memory"\n') as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session()))
            self.assertEqual(h.requests[0]['prompt_cache_retention'], 'in_memory')
            self.assertNotIn('prompt_cache_options', h.requests[0])
            self.assertNotIn('service_tier', h.requests[0])

    async def test_ttl_override_is_explicit_and_wrong_generation_fails_before_post(self):
        async with Harness(lambda_body, extra='provider_profile = "openai"\nprompt_cache_ttl = "12h"\n') as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session()))
            self.assertEqual(h.requests[0]['prompt_cache_options']['ttl'], '12h')
        async with Harness(lambda_body, extra='provider_profile = "openai"\nprompt_cache_retention = "24h"\n') as h:
            await h.initialize()
            self.assertIn('prompt_cache_ttl', (await h.prompt(await h.session()))['error']['message'])
            self.assertEqual(h.requests, [])

    async def test_custom_provider_does_not_receive_openai_or_router_fields(self):
        async with Harness(lambda_body) as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session()))
            for key in ['service_tier', 'prompt_cache_key', 'prompt_cache_options', 'prompt_cache_retention', 'cache_control']:
                self.assertNotIn(key, h.requests[0])
            self.assertNotIn('x-session-id', h.request_headers[0])
        async with Harness(lambda_body, extra='service_tier = "fast"\n') as h:
            await h.initialize()
            self.assertIn('error', await h.prompt(await h.session()))
            self.assertEqual(h.requests, [])

    async def test_router_affinity_is_stable_and_cache_controls_are_specific(self):
        for model in ['anthropic/claude-sonnet-4.6', 'google/gemini-3-pro']:
            async with Harness(lambda_body, extra=f'provider_profile = "openrouter"\nmodel = "{model}"\nwire_api = "chat_completions"\n') as h:
                # Return chat protocol for this fixture.
                async def complete(body):
                    return chat({'choices': [{'delta': {'content': 'ok'}, 'finish_reason': 'stop'}]}, done=True)
                h.handler = complete
                await h.initialize()
                first, second = await h.session(), await h.session()
                for session in [first, first, second]:
                    self.assertNotIn('error', await h.prompt(session))
                keys = [headers['x-session-id'] for headers in h.request_headers]
                self.assertEqual(keys[0], keys[1])
                self.assertNotEqual(keys[0], keys[2])
                self.assertLessEqual(len(keys[0]), 256)
                self.assertEqual('cache_control' in h.requests[0], model.startswith('anthropic/'))
                self.assertNotIn('prompt_cache_options', h.requests[0])

    async def test_chat_clean_eof_requires_valid_finish_marker_and_reports_usage(self):
        events = chat({'choices': [{'delta': {'content': 'ok'}, 'finish_reason': 'stop'}], 'service_tier': 'default'},
                      {'choices': [], 'usage': {'prompt_tokens': 2000, 'prompt_tokens_details': {'cached_tokens': 1200, 'cache_write_tokens': 500}, 'completion_tokens': 20}})
        async with ScriptedHarness([(200, {}, events)], extra='wire_api = "chat_completions"\nprovider_profile = "openai"\nservice_tier = "fast"\n') as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session()))
            usages = [u['params']['usage'] for u in h.updates if u.get('method') == '_harness/usage']
            self.assertEqual(usages[-1]['cachedInputTokens'], 1200)
            self.assertEqual(usages[-1]['cacheWriteInputTokens'], 500)
            self.assertEqual(usages[-1]['serviceTier'], 'default')
            stats = (await h.rpc('_harness/stats', {}))['result']
            self.assertEqual(stats['cacheWriteInputTokens'], 500)
        broken = chat({'choices': [{'delta': {'content': 'partial'}, 'finish_reason': None}]})
        async with ScriptedHarness([(200, {}, broken)], extra='wire_api = "chat_completions"\n') as h:
            await h.initialize()
            self.assertIn('error', await h.prompt(await h.session()))
            self.assertEqual(len(h.requests), 1)

    async def test_quota_and_explicit_retry_denial_are_terminal_and_sanitized(self):
        for headers, body in [({'x-should-retry': 'true'}, '{"error":{"code":"insufficient_quota","message":"PRIVATE-PROMPT"}}'),
                              ({'x-should-retry': 'false'}, 'PRIVATE-PROMPT')]:
            async with ScriptedHarness([(429, headers, body)]) as h:
                await h.initialize()
                result = await h.prompt(await h.session())
                self.assertIn('error', result)
                self.assertNotIn('PRIVATE-PROMPT', json.dumps(result))
                self.assertEqual(len(h.requests), 1)

    async def test_fractional_and_millisecond_retry_advice_reuses_identical_body(self):
        for headers in [{'retry-after-ms': '25'}, {'Retry-After': '0.025'}]:
            async with ScriptedHarness([(503, headers, 'busy'), (200, {}, final())]) as h:
                await h.initialize()
                started = time.monotonic()
                self.assertNotIn('error', await h.prompt(await h.session()))
                self.assertGreaterEqual(time.monotonic() - started, .025)
                self.assertEqual(h.raw_requests[0], h.raw_requests[1])

    async def test_malformed_later_arguments_block_entire_mutating_batch(self):
        first = call('write_file', {'path': 'must-not-exist.txt', 'content': 'bad'}, 'write')
        second = call('write_file', {}, 'invalid')
        second['arguments'] = '{'
        async with ScriptedHarness([(200, {}, response([first, second]))]) as h:
            await h.initialize()
            self.assertIn('error', await h.prompt(await h.session()))
            self.assertFalse((h.workspace / 'must-not-exist.txt').exists())
            self.assertFalse(h.permissions)
            self.assertEqual(len(h.requests), 1)

    async def test_no_automatic_retry_or_tool_after_invalid_stream_finish(self):
        delta = {'tool_calls': [{'index': 0, 'id': 'write', 'function': {'name': 'write_file', 'arguments': '{"path":"bad","content":"bad"}'}}]}
        cases = [chat({'choices': [{'delta': delta, 'finish_reason': 'stop'}]}, done=True),
                 chat({'choices': [{'delta': {}, 'finish_reason': 'stop'}]}, {'choices': [{'delta': delta, 'finish_reason': 'tool_calls'}]}, done=True)]
        for events in cases:
            async with ScriptedHarness([(200, {}, events)], extra='wire_api = "chat_completions"\n') as h:
                await h.initialize()
                self.assertIn('error', await h.prompt(await h.session()))
                self.assertFalse((h.workspace / 'bad').exists())
                self.assertEqual(len(h.requests), 1)

    async def test_ambiguous_post_disconnect_is_not_retried(self):
        async with ScriptedHarness(["disconnect"]) as h:
            await h.initialize()
            self.assertIn('error', await h.prompt(await h.session()))
            self.assertEqual(len(h.requests), 1)

    async def test_overload_529_retries_but_nested_spend_cap_does_not(self):
        async with ScriptedHarness([(529, {'Retry-After': '0'}, '{"error":{"type":"overloaded_error"}}'), (200, {}, final())]) as h:
            await h.initialize()
            self.assertNotIn('error', await h.prompt(await h.session()))
            self.assertEqual(len(h.requests), 2)
        body = '{"error":{"type":"rate_limit_error","details":{"error_code":"enforced_spend_limit_reached"}}}'
        async with ScriptedHarness([(429, {}, body)]) as h:
            await h.initialize()
            self.assertIn('quota or billing', (await h.prompt(await h.session()))['error']['message'])
            self.assertEqual(len(h.requests), 1)


async def lambda_body(body):
    return final()


if __name__ == '__main__':
    unittest.main()
