DELETE FROM users;
INSERT INTO users (user_data) VALUES
    ('{"name": "a", "lang": [ ],
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
  "instanceTypeId": "6312d172-f0cf-40f6-b27d-9fa8feaf332f"
 }'),
    ('{"name": "b", "lang": ["en" ],
  "alternativeTitles": [
    {
      "alternativeTitle": "First alternative title"
    },
    {
      "alternativeTitle": "Second alternative title"
    },
    {
      "alternativeTitle": "a; b; 2b94-4982-af-de-20; c"
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
  "instanceTypeId": "6312d172-f0cf-40f6-b27d-9fa8feaf332f"
 }'),
    ('{"name": "c", "lang": ["en-us"                           ] }'),
    ('{"name": "d", "lang": ["en-uk"                           ] }'),
    ('{"name": "e", "lang": ["uk"                              ] }'),
    ('{"name": "f", "lang": ["\"en"                            ] }'),
    ('{"name": "g", "lang": ["en\""                            ] }'),
    ('{"name": "h", "lang": ["\"en\""                          ] }'),
    ('{"name": "i", "lang": ["au", "ar", "at", "de", "dk", "en"] }'),
    ('{"name": "n"                                               }');
