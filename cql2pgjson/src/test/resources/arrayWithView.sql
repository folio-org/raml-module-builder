DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "aa", "lang": [ ],
  "contributors": [
    {
      "contributorNameTypeId": "2b94c631-fca9-4892-a730-03ee529ffe2a",
      "name": "Pratchett, Terry",
      "lang": "english"
    }
  ],
  "identifiers": [
    {
      "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422",
      "value": "0552142352"
    },
    {
      "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422",
      "value": "9780552142352"
    }
  ],
  "instanceTypeId": "6312d172-f0cf-40f6-b27d-9fa8feaf332f","groupId":"426cbc2a-395b-43eb-993e-70bf329a8a13"
 }'),
    ('{"name": "b", "lang": ["en" ],
  "alternativeTitles": [
    {
      "alternativeTitle": "First alternative title"
    },
    {
      "alternativeTitle": "Second alternative title"
    }
  ],
  "identifiers": [
    { "identifierTypeId": "7f907515-a1bf-4513-8a38-92e1a07c539d",
      "value": "B01LO7PJOE"
    }
  ],
  "contributors": [
    {
      "contributorNameTypeId": "2e48e713-17f3-4c13-a9f8-23845bb210aa",
      "name": "Creator A",
      "lang": "english"
    },
    {
      "contributorNameTypeId": "e8b311a6-3b21-43f2-a269-dd9310cb2d0a",
      "name": "Creator B",
      "lang": "english"
    }
  ],
  "contactInformation" : {
    "phone" : [
      { "type" : "mobile", "number" : "0912212" },
      { "type" : "home", "number" : "0912213" }
    ]
  },
  "instanceTypeId": "6312d172-f0cf-40f6-b27d-9fa8feaf332f","groupId": "ab7cbee3-657a-4648-ace1-16240136acea"
 }'),
    ('{"name": "c", "lang": ["en-us"                           ] }'),
    ('{"name": "d", "lang": ["en-uk"                           ] }'),
    ('{"name": "e", "lang": ["uk"                              ] }'),
    ('{"name": "f", "lang": ["\"en"                            ] }'),
    ('{"name": "g", "lang": ["en\""                            ] }'),
    ('{"name": "h", "lang": ["\"en\""                          ] }'),
    ('{"name": "i", "lang": ["au", "ar", "at", "de", "dk", "en"] }'),
    ('{"name": "n"                                               }');
    
    DELETE FROM groups;
INSERT INTO groups (id,group_data) VALUES
    ('426cbc2a-395b-43eb-993e-70bf329a8a13','{"name": "groupa","personId":"a708811d-422b-43fd-8fa7-d73f26dee1f9"}'),
    ('ab7cbee3-657a-4648-ace1-16240136acea','{"name": "groupb","personId":"6bb4bf61-75c6-4616-b82e-e2970e7975cf"}');
