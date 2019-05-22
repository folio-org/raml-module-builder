DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "a", "email": "\\",       "address": {"city": "*?*",       "zip": 1  }, "lang": ["en", "pl"]}'),
    ('{"name": "b", "email": "\\\\",     "address": {"city": "*?*",       "zip": 2  }, "lang": ["en", "pl"]}'),
    ('{"name": "c", "email": "*",        "address": {"city": "?\\?",      "zip": 3  }, "lang": ["en", "dk", "fi"]}'),
    ('{"name": "d", "email": "**",       "address": {"city": "?\\?",      "zip": 4  }, "lang": ["en", "dk", "fi"]}'),
    ('{"name": "e", "email": "?",        "address": {"city": "1234",      "zip": 4.0}, "lang": ["en", "dk"]}'),
    ('{"name": "f", "email": "??",       "address": {"city": "\"1234\"",  "zip": 4e0}, "lang": ["en", "dk"]}'),
    ('{"name": "g", "email": "\\\"",     "address": {"city": "01234",     "zip":17  }, "lang": ["en", "dk"]}'),
    ('{"name": "h", "email": "\\\"\\\"", "address": {"city": "\"01234\"", "zip":18  }, "lang": ["en", "dk"]}');
