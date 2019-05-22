DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "a", "lang": [                                  ] }'),
    ('{"name": "b", "lang": ["en"                              ] }'),
    ('{"name": "c", "lang": ["en-us"                           ] }'),
    ('{"name": "d", "lang": ["en-uk"                           ] }'),
    ('{"name": "e", "lang": ["uk"                              ] }'),
    ('{"name": "f", "lang": ["\"en"                            ] }'),
    ('{"name": "g", "lang": ["en\""                            ] }'),
    ('{"name": "h", "lang": ["\"en\""                          ] }'),
    ('{"name": "i", "lang": ["au", "ar", "at", "de", "dk", "en"] }'),
    ('{"name": "n"                                               }');
