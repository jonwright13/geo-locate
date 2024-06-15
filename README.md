# Geo Locate

Script using the [Geocode API](https://geocode.maps.co/) to retrieve address data with a provided table of latitude and longitude coordinates for cleaning the data in [UFO Sightings](https://github.com/jonwright13/ufo-sightings)

## Features

- Rate-limiting to prevent throttling.
- Automatic backup to restore progress if connection is interrupted
- Returned addresses are output as a csv and JSON.
