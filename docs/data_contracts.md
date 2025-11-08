# Data Contracts

This document defines the data contracts for external APIs used in the data warehouse. These contracts specify the exact field names, types, and structures to ensure deterministic transformations.

## Table of Contents

1. [Open-Meteo Weather API](#open-meteo-weather-api)
2. [Wikipedia MediaWiki REST API](#wikipedia-mediawiki-rest-api)

---

## Open-Meteo Weather API

### Overview
Open-Meteo provides free weather forecast data via a keyless API. We use the hourly forecast endpoint to retrieve weather observations.

### Endpoint
```
GET https://api.open-meteo.com/v1/forecast
```

### Request Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `latitude` | float | Yes | Latitude coordinate | `42.3601` |
| `longitude` | float | Yes | Longitude coordinate | `-71.0589` |
| `hourly` | string | Yes | Comma-separated list of hourly variables | `temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m` |
| `timezone` | string | Yes | Timezone for timestamps | `UTC` |
| `start_date` | string | No | Start date (YYYY-MM-DD) | `2024-11-08` |
| `end_date` | string | No | End date (YYYY-MM-DD) | `2024-11-09` |

### Example Request
```
GET https://api.open-meteo.com/v1/forecast?latitude=42.3601&longitude=-71.0589&hourly=temperature_2m,relativehumidity_2m,precipitation,cloudcover,windspeed_10m&timezone=UTC
```

### Response Structure

The API returns a JSON object with the following structure:

```json
{
  "latitude": 42.3601,
  "longitude": -71.0589,
  "generationtime_ms": 0.123,
  "utc_offset_seconds": 0,
  "timezone": "UTC",
  "timezone_abbreviation": "UTC",
  "elevation": 43.0,
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "°C",
    "relativehumidity_2m": "%",
    "precipitation": "mm",
    "cloudcover": "%",
    "windspeed_10m": "km/h"
  },
  "hourly": {
    "time": ["2024-11-08T00:00", "2024-11-08T01:00", ...],
    "temperature_2m": [15.2, 14.8, ...],
    "relativehumidity_2m": [65.0, 67.0, ...],
    "precipitation": [0.0, 0.0, ...],
    "cloudcover": [45.0, 50.0, ...],
    "windspeed_10m": [12.5, 13.2, ...]
  }
}
```

### Data Contract: Field Names and Types

| Field Name | Type | Description | Unit | Nullable |
|------------|------|-------------|------|----------|
| `time` | string (ISO8601) | Timestamp of observation | ISO8601 format | No |
| `temperature_2m` | float | Temperature at 2m height | Celsius (°C) | Yes |
| `relativehumidity_2m` | float | Relative humidity at 2m height | Percent (%) | Yes |
| `precipitation` | float | Precipitation amount | Millimeters (mm) | Yes |
| `cloudcover` | float | Cloud cover percentage | Percent (%) | Yes |
| `windspeed_10m` | float | Wind speed at 10m height | Kilometers per hour (km/h) | Yes |

### Transformation Notes

- The `time` field is an array of ISO8601 timestamps
- Each weather variable has a corresponding array of values
- Arrays are aligned by index (i.e., `time[0]` corresponds to `temperature_2m[0]`, etc.)
- Missing values may be represented as `null` in the JSON
- All timestamps are in UTC timezone
- Temperature values are in Celsius
- Wind speed values are in km/h

### Example Response Snippet

```json
{
  "hourly": {
    "time": [
      "2024-11-08T00:00",
      "2024-11-08T01:00",
      "2024-11-08T02:00"
    ],
    "temperature_2m": [15.2, 14.8, 14.5],
    "relativehumidity_2m": [65.0, 67.0, 68.0],
    "precipitation": [0.0, 0.0, 0.2],
    "cloudcover": [45.0, 50.0, 55.0],
    "windspeed_10m": [12.5, 13.2, 12.8]
  }
}
```

---

## Wikipedia MediaWiki REST API

### Overview
Wikipedia provides free access to page metadata via the MediaWiki REST API. We use the page endpoint to retrieve page information including page ID, latest revision ID, title, and content size.

### Endpoint
```
GET https://en.wikipedia.org/api/rest_v1/page/summary/{title}
```

For HTML content size:
```
GET https://en.wikipedia.org/api/rest_v1/page/html/{title}
```

### Request Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `title` | string | Yes | Wikipedia page title (URL-encoded) | `Boston` or `St._Louis` |

### Example Requests
```
GET https://en.wikipedia.org/api/rest_v1/page/summary/Boston
GET https://en.wikipedia.org/api/rest_v1/page/html/Boston
```

### Response Structure

#### Summary Endpoint Response

```json
{
  "type": "standard",
  "title": "Boston",
  "displaytitle": "Boston",
  "namespace": {
    "id": 0,
    "text": ""
  },
  "wikibase_item": "Q100",
  "titles": {
    "canonical": "Boston",
    "normalized": "Boston",
    "display": "Boston"
  },
  "pageid": 12345,
  "thumbnail": {
    "source": "https://upload.wikimedia.org/...",
    "width": 640,
    "height": 480
  },
  "originalimage": {
    "source": "https://upload.wikimedia.org/...",
    "width": 2000,
    "height": 1500
  },
  "lang": "en",
  "dir": "ltr",
  "revision": "123456789",
  "tid": "abc123-def456-...",
  "timestamp": "2024-11-08T12:00:00Z",
  "description": "Capital and largest city of Massachusetts",
  "content_urls": {
    "desktop": {
      "page": "https://en.wikipedia.org/wiki/Boston"
    },
    "mobile": {
      "page": "https://en.m.wikipedia.org/wiki/Boston"
    }
  },
  "extract": "Boston is the capital and most populous city...",
  "extract_html": "<p>Boston is the capital and most populous city...</p>"
}
```

#### HTML Endpoint Response

The HTML endpoint returns the full rendered HTML content of the page. The content size can be determined from the response body length.

### Data Contract: Field Names and Types

| Field Name | Type | Description | Source | Nullable |
|------------|------|-------------|--------|----------|
| `pageid` | integer | Wikipedia page ID | `pageid` from summary | No |
| `revision` | string | Latest revision ID | `revision` from summary | No |
| `title` | string | Page title | `title` from summary | No |
| `content_size_bytes` | integer | Size of rendered HTML content | Length of HTML endpoint response body | No |

### Transformation Notes

- The `pageid` is a unique identifier for the Wikipedia page
- The `revision` field contains the latest revision ID as a string
- The `title` field contains the canonical page title
- The `content_size_bytes` is calculated from the HTML endpoint response:
  - Make a GET request to `/api/rest_v1/page/html/{title}`
  - Calculate the byte length of the response body
- Page titles should be URL-encoded when used in the API endpoint
- Spaces in titles should be replaced with underscores (e.g., "St. Louis" → "St._Louis")
- The API returns data in JSON format
- All timestamps are in ISO8601 format with UTC timezone

### Example Response Snippet

```json
{
  "pageid": 12345,
  "title": "Boston",
  "revision": "123456789",
  "timestamp": "2024-11-08T12:00:00Z"
}
```

### Content Size Calculation

To get the content size:
1. Request: `GET https://en.wikipedia.org/api/rest_v1/page/html/Boston`
2. Response: Full HTML content as string
3. Calculate: `content_size_bytes = len(response_body.encode('utf-8'))`

### Error Handling

- If a page doesn't exist, the API returns a 404 status
- Invalid titles may return 400 status
- Rate limiting may apply (check response headers)
- Missing fields should be handled gracefully in transformations

---

## Data Contract Summary

### Weather Data (Open-Meteo)
- **Source**: Open-Meteo Hourly Forecast API
- **Fields**: time, temperature_2m, relativehumidity_2m, precipitation, cloudcover, windspeed_10m
- **Format**: JSON with aligned arrays
- **Timezone**: UTC

### Wikipedia Data (MediaWiki REST API)
- **Source**: Wikipedia REST API v1
- **Fields**: pageid, revision, title, content_size_bytes
- **Format**: JSON
- **Endpoints**: `/page/summary/{title}` and `/page/html/{title}`

---

## Version History

- **2024-11-08**: Initial data contract definitions

