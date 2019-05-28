DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "a", "email": "\""   }'),
    ('{"name": "b", "email": "a\"b" }'),
    ('{"name": "c", "email": "\"\\" }');
