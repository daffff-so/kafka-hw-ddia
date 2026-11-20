import json
import unittest

from app.common import decode_event, json_bytes


class EventCodecTest(unittest.TestCase):
    def test_round_trip(self):
        event = {
            "event_id": "id",
            "user_id": "user-1",
            "type": "page_view",
            "seq": 1,
            "produced_at": "2026-01-01T00:00:00+00:00",
            "payload": {"poison": False},
        }
        self.assertEqual(event, decode_event(json_bytes(event)))

    def test_missing_field_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "missing event fields"):
            decode_event(json.dumps({"event_id": "id"}).encode())


if __name__ == "__main__":
    unittest.main()

