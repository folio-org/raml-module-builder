DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "n"                 }'),
    ('{"name": "e1", "email": null }'),
    ('{"name": "e2", "email": ""   }'),
    ('{"name": "e3", "email": "  " }'),
    ('{"name": "e4", "email": "e"  }'),
    ('{"name": "c0", "address": {              } }'),
    ('{"name": "c1", "address": { "city": null } }'),
    ('{"name": "c2", "address": { "city": ""   } }'),
    ('{"name": "c3", "address": { "city": "  " } }'),
    ('{"name": "c4", "address": { "city": "c"  } }');
