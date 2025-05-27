# Demography Data Explorer (Gloodata Extension)

A Python extension for [Gloodata](https://gloodata.com/) that displays fertility and demographic data from [Our World in Data: Population & Demography Data Explorer](https://ourworldindata.org/explorers/population-and-demography).

![Extension Preview](https://raw.githubusercontent.com/gloodata/extension-demography/refs/heads/main/resources/ext-preview.webp)

## Tools

- 🌍 **World Map** - Fertility rates across countries by year
- 📊 **Population Pyramid** - Age distribution by gender for any country and year
- 📈 **Time Series Analysis** - Demographic changes over time by age groups
- ℹ️ **Country Information**
- 📇 **Country Directory** - Table of all countries with regional information

## Technologies

- Python
- [DuckDB](https://duckdb.org/)
- [uv](https://docs.astral.sh/uv/)

## Data Sources

The project works with three main datasets:
- `countries.csv` - Country metadata (ISO codes, regions, etc.)
- `demography-*.csv` - Raw demography data
- `demography.parquet` - Population data by age groups, country, and year
  - Processed with `datawrangler.py`
- `fertility-rate.csv` - Fertility rates by country and year

## Setup and Installation

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/)

Check that you are in a recent version of `uv`:

```bash
uv self update
```

### Project Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/gloodata/extension-demography.git
   cd extension-demography
   ```

2. **Run the extension**:
   ```bash
   uv run src/main.py
   ```

Available environment variables and their defaults:

- `EXTENSION_PORT`: `8089`
- `EXTENSION_HOST`: `localhost`

For example, to change the port:

```sh
EXTENSION_PORT=6677 uv run src/main.py
```

## Available Visualizations

### 1. Population Pyramids

Interactive pyramid chart showing male/female population by age groups

- "population pyramid for spain in 2023"
- "demography for india in 1950"

Tool: `demography_by_country_and_year`

### 2. World Fertility Map

Choropleth world map with color-coded fertility rates

- "world fertility for 2023"
- "fertility in 1950"

Tool: `world_fertility_by_year`

### 3. Demographic Time Series

Multi-line chart showing population trends by age group

- "Demography timeserie for Spain"
- "Italy's demography over the years"

Tool: `demography_by_country_over_time`

### 4. Country Information

Get detailed country information:

- "Information about Spain"
- "Italy's info"

Tool: `country_infobox`

### 5. Country Directory

Browse all available countries:

- "Show country table"

Tool: `country_table`

## Development

### Project Structure

Files you may want to check first:

```
extension-demography/
├── src/
│   └── toolbox.py          # Main extension logic
├── datawrangler.py         # Script for merging `demography-*.csv` files into `demography.parquet`
└── queries.sql             # SQL queries
```

### Regenerating demography.parquet


```sh
rm -f ./demography.parquet
uv run datawrangler.py merge-demography-csvs -m ./demography-male.csv -f ./demography-female.csv -b ./demography-both.csv -o ./demography.parquet
```

For more information about the script:

```sh
uv run datawrangler.py -h
```

### Adding New Visualizations

1. Define new SQL queries in `queries.sql`
2. Create tool functions in `src/toolbox.py` using the `@tb.tool` decorator
3. Specify visualization types and parameters in the return dictionary

## License

This project is open source and available under the [MIT License](LICENSE).

## Support

For questions, issues, or contributions, please open an issue on GitHub or contact the maintainers.
