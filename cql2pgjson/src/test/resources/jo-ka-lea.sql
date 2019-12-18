DELETE FROM groups;
INSERT INTO groups VALUES
  ('77777777-7777-7777-7777-777777777777','{"id":"77777777-7777-7777-7777-777777777777"}'),
  ('88888888-8888-8888-8888-888888888888','{"id":"88888888-8888-8888-8888-888888888888"}'),
  ('99999999-9999-9999-9999-999999999999','{"id":"99999999-9999-9999-9999-999999999999"}');
DELETE FROM users;
INSERT INTO users (id,user_data,groupId) VALUES
  ('11111111-1111-1111-1111-111111111111','{"id":"11111111-1111-1111-1111-111111111111",
    "name": "Jo Jane", "status": "Active - Ready", "email": "jo@example.com",
    "alternateEmail": "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com",
    "address": {"city": "Sydhavn", "zip": 2450}, "lang": ["en", "pl"], "number": 4}', '77777777-7777-7777-7777-777777777777'),
  ('22222222-2222-2222-2222-222222222222','{"id":"22222222-2222-2222-2222-222222222222",
    "name": "Ka Keller", "status": "Inactive", "email": "ka@example.com",
    "alternateEmail": "ffgffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff@example.com",
    "address": {"city": "Fred", "zip": 1900}, "lang": ["en", "dk", "fi"]}', '88888888-8888-8888-8888-888888888888'),
  ('33333333-3333-3333-3333-33333333333a','{"id":"33333333-3333-3333-3333-333333333333",
    "name": "Lea Long", "status": "Active - Not Yet", "email": "lea@example.com",
    "alternateEmail": "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com",
    "address": {"city": "Søvang", "zip": 2791}, "lang": ["en", "dk"]}', '99999999-9999-9999-9999-999999999999');
