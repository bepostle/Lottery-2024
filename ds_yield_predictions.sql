select *
FROM raw_data_science.raw_yield_forecasting_applications_tlnd
WHERE ds = (select max(ds) FROM raw_data_science.raw_yield_forecasting_applications_tlnd)