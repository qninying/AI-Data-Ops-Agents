USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'OilfieldOps')
    CREATE DATABASE OilfieldOps;
GO

USE OilfieldOps;
GO

IF OBJECT_ID('dbo.oil_fields', 'U') IS NULL
CREATE TABLE dbo.oil_fields (
    field_id         INT           PRIMARY KEY,
    field_name       VARCHAR(100)  NOT NULL,
    play             VARCHAR(100)  NOT NULL,
    basin            VARCHAR(100)  NOT NULL,
    county           VARCHAR(60)   NOT NULL,
    state_code       CHAR(2)       NOT NULL DEFAULT 'OK',
    primary_operator VARCHAR(100)  NULL,
    active           BIT           NOT NULL DEFAULT 1
);
GO

IF OBJECT_ID('dbo.wells', 'U') IS NULL
CREATE TABLE dbo.wells (
    well_id          INT IDENTITY(1,1) PRIMARY KEY,
    api_number       VARCHAR(18)   NOT NULL UNIQUE,
    well_name        VARCHAR(150)  NOT NULL,
    field_id         INT           NOT NULL REFERENCES dbo.oil_fields(field_id),
    well_type        VARCHAR(20)   NOT NULL CHECK (well_type IN ('PRODUCER','INJECTOR','DISPOSAL','OBSERVATION')),
    completion_type  VARCHAR(20)   NULL,
    spud_date        DATE          NULL,
    first_prod_date  DATE          NULL,
    latitude         DECIMAL(9,6)  NULL,
    longitude        DECIMAL(9,6)  NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE'
                                   CHECK (status IN ('ACTIVE','INACTIVE','PLUGGED','SHUT_IN'))
);
GO

IF OBJECT_ID('stg.daily_production', 'U') IS NULL
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
        EXEC('CREATE SCHEMA stg');

    CREATE TABLE stg.daily_production (
        row_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        source_file         VARCHAR(200)  NULL,
        ingest_timestamp    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        api_number          VARCHAR(18)   NULL,
        report_date         DATE          NULL,
        oil_bbls            DECIMAL(10,2) NULL,
        gas_mcf             DECIMAL(10,3) NULL,
        water_bbls          DECIMAL(10,2) NULL,
        hours_on_production DECIMAL(5,2)  NULL,
        downtime_code       VARCHAR(10)   NULL,
        is_valid            BIT           NULL,
        validation_notes    VARCHAR(500)  NULL
    );
END
GO

IF OBJECT_ID('dbo.daily_production', 'U') IS NULL
CREATE TABLE dbo.daily_production (
    production_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    api_number          VARCHAR(18)   NOT NULL,
    well_id             INT           NOT NULL REFERENCES dbo.wells(well_id),
    report_date         DATE          NOT NULL,
    oil_bbls            DECIMAL(10,2) NOT NULL,
    gas_mcf             DECIMAL(10,3) NOT NULL,
    water_bbls          DECIMAL(10,2) NOT NULL,
    hours_on_production DECIMAL(5,2)  NOT NULL,
    downtime_code       VARCHAR(10)   NULL,
    etl_run_id          INT           NOT NULL,
    loaded_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_prod_well_date UNIQUE (api_number, report_date)
);
GO

IF OBJECT_ID('dbo.etl_run_log', 'U') IS NULL
CREATE TABLE dbo.etl_run_log (
    run_id          INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name   VARCHAR(100)  NOT NULL,
    run_start       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    run_end         DATETIME2     NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'RUNNING',
    source_file     VARCHAR(200)  NULL,
    rows_received   INT           NULL,
    rows_loaded     INT           NULL,
    rows_rejected   INT           NULL,
    wells_expected  INT           NULL,
    coverage_pct    DECIMAL(5,2)  NULL,
    null_rate_pct   DECIMAL(5,2)  NULL,
    failure_mode    VARCHAR(50)   NULL,
    error_message   NVARCHAR(MAX) NULL,
    schema_hash     VARCHAR(64)   NULL,
    notes           NVARCHAR(MAX) NULL,
    ai_incident_report NVARCHAR(MAX) NULL,
    ai_fix_sql         NVARCHAR(MAX) NULL
);
GO

-- Seed oil fields
IF NOT EXISTS (SELECT 1 FROM dbo.oil_fields)
INSERT INTO dbo.oil_fields (field_id, field_name, play, basin, county, primary_operator)
VALUES
    (1, 'Kingfisher STACK',       'STACK',          'Anadarko Basin', 'Kingfisher', 'Continental Resources'),
    (2, 'Canadian County STACK',  'STACK',          'Anadarko Basin', 'Canadian',   'Devon Energy'),
    (3, 'Blaine Springer',        'Springer',       'Anadarko Basin', 'Blaine',     'Chaparral Energy'),
    (4, 'Caddo Woodford',         'Woodford Shale', 'Anadarko Basin', 'Caddo',      'SandRidge Energy'),
    (5, 'Garfield Mississippian', 'Mississippian',  'Anadarko Basin', 'Garfield',   'SandRidge Energy'),
    (6, 'Grant County Meramec',   'STACK',          'Anadarko Basin', 'Grant',      'Continental Resources'),
    (7, 'Logan Hunton',           'Hunton',         'Anadarko Basin', 'Logan',      'Unit Petroleum'),
    (8, 'Major County Oswego',    'Oswego',         'Anadarko Basin', 'Major',      'Chaparral Energy');
GO

-- Seed wells
IF NOT EXISTS (SELECT 1 FROM dbo.wells)
INSERT INTO dbo.wells (api_number, well_name, field_id, well_type, completion_type, spud_date, first_prod_date, latitude, longitude)
VALUES
    ('35073-21450-00-00', 'Kingfisher 1-27H',     1, 'PRODUCER',    'HORIZONTAL', '2019-03-10', '2019-09-01', 35.8621, -97.9354),
    ('35073-21451-00-00', 'Kingfisher 2-27H',     1, 'PRODUCER',    'HORIZONTAL', '2019-04-15', '2019-10-12', 35.8634, -97.9401),
    ('35073-21452-00-00', 'Kingfisher 3-34H',     1, 'PRODUCER',    'HORIZONTAL', '2020-01-20', '2020-07-08', 35.8599, -97.9312),
    ('35017-31100-00-00', 'Canadian 1-15H',        2, 'PRODUCER',    'HORIZONTAL', '2018-06-01', '2018-12-01', 35.5423, -97.8891),
    ('35017-31101-00-00', 'Canadian 2-15H',        2, 'PRODUCER',    'HORIZONTAL', '2018-07-14', '2019-01-20', 35.5441, -97.8923),
    ('35017-31102-00-00', 'Canadian 3-22H',        2, 'PRODUCER',    'HORIZONTAL', '2019-11-05', '2020-05-15', 35.5389, -97.8856),
    ('35011-40201-00-00', 'Blaine 1-8H',           3, 'PRODUCER',    'HORIZONTAL', '2017-09-22', '2018-03-10', 35.9812, -98.4521),
    ('35011-40202-00-00', 'Blaine 2-8H',           3, 'PRODUCER',    'HORIZONTAL', '2017-10-30', '2018-04-22', 35.9834, -98.4578),
    ('35011-40203-00-00', 'Blaine SWD-1',          3, 'DISPOSAL',    'VERTICAL',   '2016-04-01', NULL,         35.9756, -98.4490),
    ('35023-50301-00-00', 'Caddo Woodford 1-19H',  4, 'PRODUCER',    'HORIZONTAL', '2020-02-14', '2020-08-30', 35.1234, -98.3412),
    ('35023-50302-00-00', 'Caddo Woodford 2-19H',  4, 'PRODUCER',    'HORIZONTAL', '2020-03-29', '2020-09-15', 35.1267, -98.3445),
    ('35047-60401-00-00', 'Garfield Miss 1-12V',   5, 'PRODUCER',    'VERTICAL',   '2015-07-18', '2015-10-01', 36.3891, -97.8234),
    ('35047-60402-00-00', 'Garfield Miss 2-12V',   5, 'PRODUCER',    'VERTICAL',   '2015-09-03', '2015-11-15', 36.3912, -97.8267),
    ('35047-60403-00-00', 'Garfield Miss 3-7H',    5, 'PRODUCER',    'HORIZONTAL', '2021-05-10', '2021-11-01', 36.3845, -97.8201),
    ('35049-70501-00-00', 'Grant Meramec 1-30H',   6, 'PRODUCER',    'HORIZONTAL', '2019-08-20', '2020-02-14', 36.5612, -97.9823),
    ('35049-70502-00-00', 'Grant Meramec 2-30H',   6, 'PRODUCER',    'HORIZONTAL', '2019-09-15', '2020-03-01', 36.5634, -97.9856),
    ('35049-70503-00-00', 'Grant Meramec 3-30H',   6, 'PRODUCER',    'HORIZONTAL', '2021-01-08', '2021-07-20', 36.5589, -97.9790),
    ('35083-80601-00-00', 'Logan Hunton 1-5V',     7, 'PRODUCER',    'VERTICAL',   '2014-03-14', '2014-06-01', 35.9123, -97.4512),
    ('35083-80602-00-00', 'Logan Hunton 2-5V',     7, 'PRODUCER',    'VERTICAL',   '2014-05-22', '2014-08-10', 35.9145, -97.4545),
    ('35093-90701-00-00', 'Major Oswego 1-18H',    8, 'PRODUCER',    'HORIZONTAL', '2022-04-01', '2022-10-15', 36.2341, -98.6712),
    ('35093-90702-00-00', 'Major Oswego 2-18H',    8, 'PRODUCER',    'HORIZONTAL', '2022-05-18', '2022-11-30', 36.2367, -98.6745),
    ('35073-21453-00-00', 'Kingfisher 4-10H',      1, 'PRODUCER',    'HORIZONTAL', '2021-03-01', '2021-09-01', 35.8712, -97.9512),
    ('35073-21454-00-00', 'Kingfisher 5-10H',      1, 'PRODUCER',    'HORIZONTAL', '2021-04-15', '2021-10-01', 35.8734, -97.9534),
    ('35017-31103-00-00', 'Canadian 4-33H',        2, 'PRODUCER',    'HORIZONTAL', '2020-08-01', '2021-02-01', 35.5501, -97.9012),
    ('35017-31104-00-00', 'Canadian SWD-1',        2, 'DISPOSAL',    'VERTICAL',   '2017-01-10', NULL,         35.5312, -97.8712),
    ('35011-40204-00-00', 'Blaine 3-16H',          3, 'PRODUCER',    'HORIZONTAL', '2021-06-20', '2021-12-01', 35.9912, -98.4623),
    ('35023-50303-00-00', 'Caddo Woodford 3-6H',   4, 'PRODUCER',    'HORIZONTAL', '2021-11-01', '2022-05-01', 35.1312, -98.3512),
    ('35047-60404-00-00', 'Garfield Obs-1',        5, 'OBSERVATION', 'VERTICAL',   '2016-02-01', NULL,         36.3978, -97.8345),
    ('35049-70504-00-00', 'Grant Meramec 4-15H',   6, 'PRODUCER',    'HORIZONTAL', '2022-02-14', '2022-08-01', 35.5678, -97.9934),
    ('35083-80603-00-00', 'Logan Hunton 3-11V',    7, 'PRODUCER',    'VERTICAL',   '2016-09-01', '2016-11-15', 35.9201, -97.4623),
    ('35093-90703-00-00', 'Major Oswego 3-25H',    8, 'PRODUCER',    'HORIZONTAL', '2023-01-15', '2023-07-01', 36.2412, -98.6823),
    ('35073-21455-00-00', 'Kingfisher 6-21H',      1, 'PRODUCER',    'HORIZONTAL', '2022-07-10', '2023-01-01', 35.8801, -97.9634),
    ('35017-31105-00-00', 'Canadian 5-27H',        2, 'PRODUCER',    'HORIZONTAL', '2022-09-01', '2023-03-01', 35.5589, -97.9123),
    ('35011-40205-00-00', 'Blaine 4-24H',          3, 'PRODUCER',    'HORIZONTAL', '2022-11-01', '2023-05-01', 35.9990, -98.4712),
    ('35023-50304-00-00', 'Caddo WFD Inj-1',       4, 'INJECTOR',    'VERTICAL',   '2018-05-01', NULL,         35.1189, -98.3289),
    ('35047-60405-00-00', 'Garfield Miss 4-19H',   5, 'PRODUCER',    'HORIZONTAL', '2023-02-01', '2023-08-01', 36.3923, -97.8123),
    ('35049-70505-00-00', 'Grant Meramec 5-22H',   6, 'PRODUCER',    'HORIZONTAL', '2023-03-15', '2023-09-01', 36.5712, -97.9923),
    ('35093-90704-00-00', 'Major Oswego 4-11H',    8, 'PRODUCER',    'HORIZONTAL', '2023-04-01', '2023-10-01', 36.2489, -98.6901),
    ('35073-21456-00-00', 'Kingfisher 7-33H',      1, 'PRODUCER',    'HORIZONTAL', '2023-05-01', '2023-11-01', 35.8890, -97.9712),
    ('35017-31106-00-00', 'Canadian 6-9H',         2, 'PRODUCER',    'HORIZONTAL', '2023-06-01', '2023-12-01', 35.5667, -97.9234),
    ('35011-40206-00-00', 'Blaine 5-30H',          3, 'PRODUCER',    'HORIZONTAL', '2023-07-01', '2024-01-01', 36.0012, -98.4801),
    ('35023-50305-00-00', 'Caddo Woodford 4-14H',  4, 'PRODUCER',    'HORIZONTAL', '2023-08-01', '2024-02-01', 35.1389, -98.3623),
    ('35047-60406-00-00', 'Garfield Miss 5-26H',   5, 'PRODUCER',    'HORIZONTAL', '2023-09-01', '2024-03-01', 36.4012, -97.8456),
    ('35049-70506-00-00', 'Grant Meramec 6-8H',    6, 'PRODUCER',    'HORIZONTAL', '2023-10-01', '2024-04-01', 36.5801, -98.0012),
    ('35083-80604-00-00', 'Logan Hunton 4-18V',    7, 'PRODUCER',    'VERTICAL',   '2018-03-01', '2018-06-01', 35.9289, -97.4712),
    ('35093-90705-00-00', 'Major Oswego 5-4H',     8, 'PRODUCER',    'HORIZONTAL', '2023-11-01', '2024-05-01', 36.2567, -98.6990),
    ('35073-21457-00-00', 'Kingfisher 8-16H',      1, 'PRODUCER',    'HORIZONTAL', '2024-01-15', '2024-07-01', 35.8978, -97.9823),
    ('35017-31107-00-00', 'Canadian 7-20H',        2, 'PRODUCER',    'HORIZONTAL', '2024-02-01', '2024-08-01', 35.5745, -97.9345),
    ('35011-40207-00-00', 'Blaine 6-12H',          3, 'PRODUCER',    'HORIZONTAL', '2024-03-01', '2024-09-01', 36.0089, -98.4890),
    ('35023-50306-00-00', 'Caddo Woodford 5-28H',  4, 'PRODUCER',    'HORIZONTAL', '2024-04-01', '2024-10-01', 35.1467, -98.3734);
GO
