parsers = [
    {
        "headers": [
            "timestamp", "location", "geoname", "population",
            "daily_confirm", "daily_suspect", "daily_death", "total_confirm", "total_death",
        ],
        "regex": r'([A-Z][a-z ]+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)',
        "label": "v2.0.0",
    },
    {
        "headers": [
            "timestamp", "location", "geoname",
            "total_confirm", "daily_confirm",
            "total_china_expose", "daily_china_expose", "total_world_expose", "daily_world_expose",
            "total_cntry_expose", "daily_cntry_expose", "total_unknw_expose", "daily_unknw_expose",
            "total_death", "daily_death",
        ],
        "regex": (r'([A-Z][a-z ]+) (\d+) \((\d+)\) (\d+) \((\d+)\) (\d+) \((\d+)\) '
                  r'(\d+) \((\d+)\) (\d+) \((\d+)\) (\d+) \((\d+)\)'),
        "label": "v2.0.1",
    },
    {
        "headers": ["timestamp", "location", "geoname", "population", "total_confirm"],
        "regex": r'([A-Z][a-z ]+) (\d+) (\d+)',
        "label": "v1.2.0",
    },
    {
        "headers": ["timestamp", "location", "geoname", "total_confirm"],
        "regex": r'([A-Z][a-z ]+) (\d+)',
        "label": "v1.2.1",
    },
    {
        "headers": [
            "timestamp", "location", "geoname", "total_confirm", "daily_confirm",
            "total_death", "daily_death"
        ],
        "regex": r'([A-Z][a-z ]+) (\d+) \((\d+)\) (\d+) \((\d+)\)',
        "label": "v1.1.0",
    }
]
