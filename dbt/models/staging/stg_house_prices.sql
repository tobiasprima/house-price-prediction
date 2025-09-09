WITH src AS (
  SELECT * FROM {{ source('house_prices_raw', 'raw_house_prices') }}
),
norm AS (
  SELECT
    -- keys / target
    "Id"                AS id,
    "SalePrice"         AS saleprice,

    -- numeric base (quoted Kaggle names)
    "GrLivArea"         AS grlivarea,
    "BsmtFullBath"      AS bsmtfullbath,
    "BsmtHalfBath"      AS bsmthalfbath,
    "FullBath"          AS fullbath,
    "HalfBath"          AS halfbath,
    "BedroomAbvGr"      AS bedroom,
    "KitchenAbvGr"      AS kitchen,
    "TotRmsAbvGrd"      AS totrmsabvgrd,
    "Fireplaces"        AS fireplaces,
    "GarageCars"        AS garagecars,
    "GarageArea"        AS garagearea,
    "WoodDeckSF"        AS wooddecksf,
    "OpenPorchSF"       AS openporchsf,
    "EnclosedPorch"     AS enclosedporch,
    "3SsnPorch"         AS threesnporch,     -- starts with digit -> must stay quoted
    "ScreenPorch"       AS screenporch,
    "PoolArea"          AS poolarea,
    "MiscVal"           AS miscval,
    "MoSold"            AS mosold,
    "YrSold"            AS yrsold,
    "YearBuilt"         AS yearbuilt,
    "YearRemodAdd"      AS yearremodadd,
    COALESCE("GarageYrBlt", 0) AS garageyrblt,

    -- some high-signal numeric Kaggle fields you mentioned (optional, keep adding as needed)
    "MSSubClass"        AS mssubclass,
    "LotArea"           AS lotarea,
    "OverallQual"       AS overallqual,
    "OverallCond"       AS overallcond,
    "1stFlrSF"          AS firstflrsf,       -- starts with digit -> quoted above, alias to safe name
    "2ndFlrSF"          AS secondflrsf,
    "TotalBsmtSF"       AS totalbsmtsf,

    -- categorical, coalesced to explicit “None/default” per Kaggle docs
    COALESCE("KitchenQual",   'None')    AS kitchenqual,
    COALESCE("Functional",    'Typ')     AS functional,
    COALESCE("FireplaceQu",   'None')    AS fireplacequ,
    COALESCE("GarageType",    'None')    AS garagetype,
    COALESCE("GarageFinish",  'None')    AS garagefinish,
    COALESCE("GarageQual",    'None')    AS garagequal,
    COALESCE("GarageCond",    'None')    AS garagecond,
    COALESCE("PoolQC",        'None')    AS poolqc,
    COALESCE("Fence",         'None')    AS fence,
    COALESCE("MiscFeature",   'None')    AS miscfeature,
    COALESCE("PavedDrive",    'N')       AS paveddrive,
    COALESCE("SaleType",      'WD')      AS saletype,
    COALESCE("SaleCondition", 'Normal')  AS salecondition
  FROM src
),
feat AS (
  SELECT
    *,
    (fullbath + 0.5*halfbath + bsmtfullbath + 0.5*bsmthalfbath) AS totalbaths,
    (yrsold - yearbuilt)    AS houseage,
    (yrsold - yearremodadd) AS remodage,
    CASE WHEN fireplaces > 0 THEN 1 ELSE 0 END AS hasfireplace,
    COALESCE(wooddecksf,0)
      + COALESCE(openporchsf,0)
      + COALESCE(enclosedporch,0)
      + COALESCE(threesnporch,0)
      + COALESCE(screenporch,0)                         AS totalporchsf
  FROM norm
),
enc AS (
  SELECT
    f.*,
    CASE kitchenqual     -- Ex, Gd, TA, Fa, Po, None
      WHEN 'Ex' THEN 5 WHEN 'Gd' THEN 4 WHEN 'TA' THEN 3 WHEN 'Fa' THEN 2
      WHEN 'Po' THEN 1 WHEN 'None' THEN 0 ELSE 0 END AS kitchenqual_enc,
    CASE functional      -- Typ, Min1, Min2, Mod, Maj1, Maj2, Sev, Sal
      WHEN 'Typ' THEN 5 WHEN 'Min1' THEN 4 WHEN 'Min2' THEN 3 WHEN 'Mod' THEN 2
      WHEN 'Maj1' THEN 1 WHEN 'Maj2' THEN 0 WHEN 'Sev' THEN -1 WHEN 'Sal' THEN -2
      ELSE 5 END AS functional_enc,
    CASE fireplacequ
      WHEN 'Ex' THEN 5 WHEN 'Gd' THEN 4 WHEN 'TA' THEN 3 WHEN 'Fa' THEN 2
      WHEN 'Po' THEN 1 WHEN 'None' THEN 0 ELSE 0 END AS fireplacequ_enc,
    CASE garagefinish
      WHEN 'Fin' THEN 3 WHEN 'RFn' THEN 2 WHEN 'Unf' THEN 1 WHEN 'None' THEN 0
      ELSE 0 END AS garagefinish_enc,
    CASE garagequal
      WHEN 'Ex' THEN 5 WHEN 'Gd' THEN 4 WHEN 'TA' THEN 3 WHEN 'Fa' THEN 2
      WHEN 'Po' THEN 1 WHEN 'None' THEN 0 ELSE 0 END AS garagequal_enc,
    CASE garagecond
      WHEN 'Ex' THEN 5 WHEN 'Gd' THEN 4 WHEN 'TA' THEN 3 WHEN 'Fa' THEN 2
      WHEN 'Po' THEN 1 WHEN 'None' THEN 0 ELSE 0 END AS garagecond_enc,
    CASE paveddrive      -- Y, P, N
      WHEN 'Y' THEN 2 WHEN 'P' THEN 1 WHEN 'N' THEN 0 ELSE 0 END AS paveddrive_enc,
    CASE poolqc          -- Ex, Gd, TA, Fa, None
      WHEN 'Ex' THEN 4 WHEN 'Gd' THEN 3 WHEN 'TA' THEN 2 WHEN 'Fa' THEN 1
      WHEN 'None' THEN 0 ELSE 0 END AS poolqc_enc
  FROM feat f
)

SELECT
  -- identifiers
  id, saleprice,

  -- numeric base
  grlivarea, bsmtfullbath, bsmthalfbath, fullbath, halfbath,
  bedroom, kitchen, totrmsabvgrd, fireplaces,
  garagecars, garagearea, wooddecksf, openporchsf, enclosedporch, threesnporch,
  screenporch, poolarea, miscval, mosold, yrsold, yearbuilt, yearremodadd, garageyrblt,
  mssubclass, lotarea, overallqual, overallcond, firstflrsf, secondflrsf, totalbsmtsf,

  -- categorical (normalized)
  kitchenqual, functional, fireplacequ, garagetype, garagefinish, garagequal,
  garagecond, poolqc, fence, miscfeature, paveddrive, saletype, salecondition,

  -- engineered
  totalbaths, houseage, remodage, hasfireplace, totalporchsf,

  -- ordinal encodings
  kitchenqual_enc, functional_enc, fireplacequ_enc, garagefinish_enc,
  garagequal_enc, garagecond_enc, paveddrive_enc, poolqc_enc
FROM enc