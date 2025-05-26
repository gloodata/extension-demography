-- name: fert_by_country(country)
SELECT country, year, fertility
FROM fertility
WHERE country=:country
ORDER BY year

-- name: fert_by_year(year)
SELECT country, year, fertility
FROM fertility
WHERE year=:year
ORDER BY country, year

-- name: countries_all()
SELECT name, alpha_2, alpha_3, code, region, sub_region, region_code, sub_region_code
FROM country
ORDER BY name

-- name: countries_by_region(region_code)
SELECT name, alpha_2, alpha_3, code, region, sub_region, region_code, sub_region_code
FROM country
WHERE region_code = :region_code
ORDER BY name

-- name: countries_by_sub_region(sub_region_code)
SELECT name, alpha_2, alpha_3, code, region, sub_region, region_code, sub_region_code
FROM country
WHERE sub_region_code = :sub_region_code
ORDER BY name

-- name: country_key_and_label_pairs()
SELECT alpha_3 AS key, name AS label
FROM country
ORDER BY label

-- name: get_country_by_fuzzy_name(name)

SELECT name, alpha_2, alpha_3, code, region, sub_region, region_code, sub_region_code
FROM country
WHERE
    name ILIKE :name OR
    alpha_3 ILIKE :name OR
    alpha_2 ILIKE :name


-- name: create_table_country(file_path)

CREATE TABLE country AS
SELECT
    name,
    "alpha-2" AS alpha_2,
    "alpha-3" AS alpha_3,
    "country-code" AS code,
    region,
    "sub-region" AS sub_region,
    "region-code" AS region_code,
    "sub-region-code" AS sub_region_code
FROM read_csv_auto(:file_path)

-- name: create_table_fertility(file_path)

CREATE TABLE fertility AS
SELECT
    "Code" AS country,
    "Year" AS year,
    "Fertility Rate" AS fertility
FROM read_csv_auto(:file_path)
WHERE country != ''

-- name: create_table_demography(file_path)

CREATE TABLE demography AS
SELECT code, year, type, years_0_4, years_5_14, years_15_24, years_25_64, years_65_plus
FROM read_parquet(:file_path)
WHERE code != ''

-- name: dem_by_country_code_year_and_type(code, year, type)
SELECT code, year, type, years_0_4, years_5_14, years_15_24, years_25_64, years_65_plus
FROM demography
WHERE code = :code AND year = :year AND type = :type

-- name: dem_by_country_code_and_type(code, type)
SELECT code, year, type, years_0_4, years_5_14, years_15_24, years_25_64, years_65_plus
FROM demography
WHERE code = :code AND type = :type
ORDER BY year

-- name: dem_by_year_and_type(year, type)
SELECT code, year, type, years_0_4, years_5_14, years_15_24, years_25_64, years_65_plus
FROM demography
WHERE year = :year AND type = :type
ORDER BY code
