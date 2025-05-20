# guatecompras-ocds-flatten

Flatten data for Guatecompras public OCDS contracts.

## Install

```
git clone
npm install
```

## Run

```
(stream of JSON lines) | node guatecompras-transformer/index.js -o [output] | (stream of JSON lines)
```

## Parameters

```
--output     -o      ocds|csv
```

When using **ocds** or no value (default) the script produces simplified objects in OCDS style, where each object corresponds to one award. When using **csv** the object produced is a much simpler, CSV style flat object with minimal data.


## Data

Works with data from the Guatemalan [OCDS API](https://ocds.guatecompras.gt/files). Preprocess by extracting all compiledReleases into JSON lines, one object per line, using a tool such as jq.
