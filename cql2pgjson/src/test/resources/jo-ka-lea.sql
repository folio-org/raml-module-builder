DELETE FROM users;
INSERT INTO users (_id,user_data) VALUES
    ('11111111-1111-1111-1111-111111111111','{"id":"11111111-1111-1111-1111-111111111111", "name": "Jo Jane", "email": "jo@example.com", "address": {"city": "Sydhavn", "zip": 2450}, "lang": ["en", "pl"], "number": 4}'),
    ('22222222-2222-2222-2222-222222222222','{"id":"22222222-2222-2222-2222-222222222222", "name": "Ka Keller", "email": "ka@example.com", "address": {"city": "Fred", "zip": 1900}, "lang": ["en", "dk", "fi"]}'),
    ('33333333-3333-3333-3333-33333333333a','{"id":"33333333-3333-3333-3333-333333333333", "name": "Lea Long", "email": "lea@example.com", "address": {"city": "Søvang", "zip": 2791}, "lang": ["en", "dk"]}');
