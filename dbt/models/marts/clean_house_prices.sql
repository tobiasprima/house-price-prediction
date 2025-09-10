SELECT
  id,
  saleprice,

  -- strong numeric predictors
  grlivarea,
  totalbaths,
  houseage,
  remodage,
  totrmsabvgrd,
  bedroom,
  garagecars,
  garagearea,
  totalporchsf,
  miscval,
  mosold,
  yrsold,

  -- high-signal numeric features
  overallqual,
  overallcond,
  lotarea,
  totalbsmtsf,
  firstflrsf,
  secondflrsf,

  -- ordinal encodings ready for models
  kitchenqual_enc,
  functional_enc,
  fireplacequ_enc,
  garagefinish_enc,
  garagequal_enc,
  garagecond_enc,
  paveddrive_enc,
  poolqc_enc,

  -- a few raw categoricals
  garagetype,
  saletype,
  salecondition

FROM {{ ref('stg_house_prices') }}
WHERE saleprice IS NOT NULL
