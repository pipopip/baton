import unittest
from queue_policy import DEFAULTS, policy_fields, policy_is_default, RESOURCE_POLICY_VERSION


class QueuePolicyTests(unittest.TestCase):
    def test_legacy_and_custom(self):
        self.assertEqual(policy_fields({}, 8), DEFAULTS)
        self.assertTrue(policy_is_default({'window_start': 0, 'window_end': 8}))
        custom = policy_fields({'foreground_slots': 0, 'timezone': 'America/New_York',
                                'window_start': '22:15', 'window_end': 7}, 4)
        self.assertEqual(custom, {'foreground_slots': 0, 'timezone': 'America/New_York',
                                  'window_start': '22:15', 'window_end': '07:00'})
        self.assertEqual(RESOURCE_POLICY_VERSION, 2)

    def test_bad_values_fail_closed(self):
        for value in ({'foreground_slots': True}, {'foreground_slots': -1}, {'foreground_slots': 4},
                      {'window_start': True}, {'window_start': '24:00'}, {'window_start': '8:00'},
                      {'window_start': 8, 'window_end': '08:00'}, {'timezone': '../bad'},
                      {'timezone': '/etc/passwd'}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                policy_fields(value, 4)
        with self.assertRaises(ValueError):
            policy_fields({'timezone': 'No/Such_Zone'}, 4, validate_zone=True)
