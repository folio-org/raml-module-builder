DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "file", "path": "C:\\Program Files\\FOLIO\\records.json"}'),
    ('{"name": "directory", "path": "C:\\Program Files\\FOLIO\\"}'),
    ('{"name": "without-backslashes", "path": "C:Program FilesFOLIOrecords.json"}');
