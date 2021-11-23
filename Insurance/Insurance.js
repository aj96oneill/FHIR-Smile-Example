/*
 * InsurancePlan.js ETL Import CSV Mapping function - Expects input in the format:
 *
 * PRODUCT_IDENTIFIER,PRODUCT_STATUS,PRODUCT_TYPE,PRODUCT_NAME,PRODUCT_ALIAS_NAME,PRODUCT_START_DATE,PRODUCT_END_DATE,
 * NETWORK,COVERAGE_TYPE,PLAN_IDENTIFIER,PLAN_TYPE,PLAN_GENERALCOST_TYPE,PLAN_GENERALCOST_COST,
 * PLAN_SPECIFICCOST_CATEGORY,PLAN_SPECIFICCOST_BENEFIT_TYPE,PLAN_SPECIFICCOST_BENEFIT_COST_TYPE,
 * PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY,PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIERS,
 * PLAN_SPECIFICCOST_BENEFIT_COST_VALUE,OWNEDBY,ADMINISTEREDBY,COVERAGE_AREA,PLAN_COVERAGE_AREA
 *
 * Hashing Function: function hashEtlImportRow(inputMap,context){return inputMap.PRODUCT_IDENTIFIER.trim();}
 */
/* Ticket #:     Assigned to:     Date Updated:

*/
function handleEtlImportRow(inputMap, context) {
  Log.info("Processing CSV row from file: " + context.filename);

  /* Create an InsurancePlan */
  var insuranceplan = ResourceBuilder.build("InsurancePlan");
  insuranceplan.id = Uuid.newPlaceholderId();

  let tag = insuranceplan.meta.add("tag");
  tag.system = "http://system.com/TAG";
  tag.code = context.filename == null ? "POST" : context.filename;
  tag.display = "Data import location";

  let lvl1Char = "[*]";
  let lvl2Char = "[=]";
  let lvl3Char = "[%]";
  let lvl4Char = "[~]";
  let lvl5Char = "[^]";

  var rollUpDelimiter = new RegExp(/\[\*\]|\[\=\]|\[\%\]|\[\~\]|\[\^\]/);

  /* Required PRODUCT_IDENTIFIER value can not be null, empty, or unknown */
  if (
    inputMap.PRODUCT_IDENTIFIER.trim().toUpperCase() == "NULL" ||
    inputMap.PRODUCT_IDENTIFIER.trim() === "" ||
    inputMap.PRODUCT_IDENTIFIER.trim().toUpperCase() == "UNKNOWN"
  ) {
    Log.error(
      "PRODUCT IDENTIFIER can not be UNKNOWN NULL or EMPTY: " +
      inputMap.PRODUCT_IDENTIFIER.trim()
    );
    return;
  }
  if (rollUpDelimiter.test(inputMap.PRODUCT_IDENTIFIER.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_IDENTIFIER: " +
      inputMap.PRODUCT_IDENTIFIER.trim()
    );
    return;
  }

  /* Lock on the PRODUCT_IDENTIFIER */
  context.lock(inputMap.PRODUCT_IDENTIFIER.trim());

  /* Identifier */
  let identifier = inputMap.PRODUCT_IDENTIFIER.trim();

  insuranceplan.identifier[0].use = "official";
  insuranceplan.identifier[0].type.coding[0].system = "http://terminology.hl7.org/CodeSystem/v2-0203";
  insuranceplan.identifier[0].type.coding[0].code = "NIIP";
  insuranceplan.identifier[0].type.coding[0].display = "National Insurance Payor Identifier";
  insuranceplan.identifier[0].type.coding[0].userSelected = true;
  insuranceplan.identifier[0].type.text = "National Insurance Payor Identifier";
  insuranceplan.identifier[0].system = "http://system.com/INSURANCEPLAN";
  insuranceplan.identifier[0].value = identifier;

  /* Check PRODUCT_START_DATE value is not NULL, empty or UNKNOWN */
  let periodStart = "NA";
  if (rollUpDelimiter.test(inputMap.PRODUCT_START_DATE.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_START_DATE: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_START_DATE */
    let lvl5Array = inputMap.PRODUCT_START_DATE.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }

    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }

    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }

    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.period.start = new Date(distinctArray[0].replaceAll("/", "-")).toISOString().substring(0, 10);
    }
  } else {
    if (
      inputMap.PRODUCT_START_DATE.trim().toUpperCase() != "NULL" &&
      inputMap.PRODUCT_START_DATE.trim().toUpperCase() != "NAN" &&
      inputMap.PRODUCT_START_DATE.trim() !== "" &&
      inputMap.PRODUCT_START_DATE.trim().toUpperCase() != "UNKNOWN"
    ) {
      periodStart = new Date(inputMap.PRODUCT_START_DATE.replaceAll("/", "-").trim()).toISOString().substring(0, 10);
      insuranceplan.period.start = periodStart;
    }
  }

  let periodEnd = "NA";

  /* Check PRODUCT_END_DATE value is not NULL, empty or UNKNOWN */
  if (rollUpDelimiter.test(inputMap.PRODUCT_END_DATE.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_END_DATE: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_END_DATE */
    let lvl5Array = inputMap.PRODUCT_END_DATE.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => new Date(elem.replaceAll("/", "-").trim()).toISOString().substring(0, 10));

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      if (new Date(inputMap.PRODUCT_START_DATE.trim().replaceAll("/", "-").trim()).toISOString().substring(0, 10) <=
        new Date(distinctArray[0])) {
        insuranceplan.period.end = new Date(distinctArray[0]).toISOString().substring(0, 10);
      }
      else {
        insuranceplan.period.end = "2199-12-31";
        Log.error(
          "period.end cannot be before period.start: " +
          identifier
        );
      }
    }
  } else {
    if (
      inputMap.PRODUCT_END_DATE.trim().toUpperCase() != "NULL" &&
      inputMap.PRODUCT_END_DATE.trim().toUpperCase() != "NAN" &&
      inputMap.PRODUCT_END_DATE.trim() !== "" &&
      inputMap.PRODUCT_END_DATE.trim().toUpperCase() != "UNKNOWN"
    ) {
      periodEnd = new Date(inputMap.PRODUCT_END_DATE.replaceAll("/", "-").trim()).toISOString().substring(0, 10);

      if (
        periodStart != "NA" &&
        periodStart <= periodEnd
      ) {
        insuranceplan.period.end = periodEnd;
      } else {
        insuranceplan.period.end = "2199-12-31";
        Log.error(
          "period.end cannot be before period.start: " +
          identifier
        );
      }
    }
  }

  if (rollUpDelimiter.test(inputMap.PRODUCT_STATUS.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_STATUS: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_STATUS */
    let lvl5Array = inputMap.PRODUCT_STATUS.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      if (distinctArray[0].toUpperCase() == "ACTIVE" || distinctArray[0].toUpperCase() == "DRAFT" ||
        distinctArray[0].toUpperCase() == "RETIRED") {
        /* 	draft | active | retired | unknown */
        insuranceplan.meta.profile[0] =
          "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-InsurancePlan";
        insuranceplan.status = "active";
      } else {
        insuranceplan.status = "unknown";
      }
    } else {
      insuranceplan.status = "unknown";
    }
  } else {
    if (inputMap.PRODUCT_STATUS.trim().toUpperCase() == "ACTIVE" || inputMap.PRODUCT_STATUS.trim().toUpperCase() == "DRAFT" ||
      inputMap.PRODUCT_STATUS.trim().toUpperCase() == "RETIRED") {
      /* 	draft | active | retired | unknown */
      insuranceplan.meta.profile[0] =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-InsurancePlan";
      insuranceplan.status = "active";
    } else {
      insuranceplan.status = "unknown";
    }
  }

  /* Type */
  /* Required PRODUCT_TYPE value can not be null, empty, or unknown */
  var productType = null;
  if (rollUpDelimiter.test(inputMap.PRODUCT_TYPE.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_TYPE: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_TYPE */
    let lvl5Array = inputMap.PRODUCT_TYPE.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      productType = distinctArray[0].toUpperCase();
    } else {
      Log.error(
        "PRODUCT TYPE can not be NULL EMPTY or UNKNOWN: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PRODUCT_TYPE.trim().toUpperCase() == "NULL" ||
      inputMap.PRODUCT_TYPE.trim().toUpperCase() == "NAN" ||
      inputMap.PRODUCT_TYPE.trim() === "" ||
      inputMap.PRODUCT_TYPE.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "PRODUCT TYPE can not be NULL EMPTY or UNKNOWN: " +
        identifier
      );
      return;
    }
    productType = inputMap.PRODUCT_TYPE.trim().toUpperCase();
  }

  switch (productType) {
    case "MEDICAL":
      insuranceplan.type[0].coding[0].system =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsuranceProductTypeCS";
      insuranceplan.type[0].coding[0].code = "qhp";
      insuranceplan.type[0].coding[0].display = "Qualified Health Plan";
      break;
    case "DENTAL":
      insuranceplan.type[0].coding[0].system =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsuranceProductTypeCS";
      insuranceplan.type[0].coding[0].code = "dent";
      insuranceplan.type[0].coding[0].display = "Dental Plan";
      break;
    case "VISION":
      insuranceplan.type[0].coding[0].system =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsuranceProductTypeCS";
      insuranceplan.type[0].coding[0].code = "vis";
      insuranceplan.type[0].coding[0].display = "Vision Plan";
      break;
    case "PHARMACY":
      insuranceplan.type[0].coding[0].system =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsuranceProductTypeCS";
      insuranceplan.type[0].coding[0].code = "medid";
      insuranceplan.type[0].coding[0].display = "Medicare Part D";
      break;
    default:
      insuranceplan.type[0].coding[0].system =
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsuranceProductTypeCS";
      insuranceplan.type[0].coding[0].code = "qhp";
      insuranceplan.type[0].coding[0].display = "Qualified Health Plan";
  }

  /* Name */
  /* Required PRODUCT_NAME value can not be null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.PRODUCT_NAME.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_NAME: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_NAME */
    let lvl5Array = inputMap.PRODUCT_NAME.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.name = distinctArray[0];
    } else {
      Log.error(
        "PRODUCT NAME can not be NULL EMPTY or UNKNOWN: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PRODUCT_NAME.trim().toUpperCase() == "NULL" ||
      inputMap.PRODUCT_NAME.trim().toUpperCase() == "NAN" ||
      inputMap.PRODUCT_NAME.trim() === "" ||
      inputMap.PRODUCT_NAME.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "PRODUCT NAME can not be NULL EMPTY or UNKNOWN: " +
        identifier
      );
      return;
    }
    insuranceplan.name = inputMap.PRODUCT_NAME.trim();
  }
  /* Alias */
  /* Required PRODUCT_ALIAS_NAME value can not be null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.PRODUCT_ALIAS_NAME.trim())) {
    Log.error(
      "Delimiter Found in PRODUCT_ALIAS_NAME: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  PRODUCT_ALIAS_NAME */
    let lvl5Array = inputMap.PRODUCT_ALIAS_NAME.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.alias[0] = distinctArray[0];
    } else {
      Log.error(
        "PRODUCT ALIAS NAME can not be NULL: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PRODUCT_ALIAS_NAME.trim().toUpperCase() == "NULL" ||
      inputMap.PRODUCT_ALIAS_NAME.trim().toUpperCase() == "NAN" ||
      inputMap.PRODUCT_ALIAS_NAME.trim() === "" ||
      inputMap.PRODUCT_ALIAS_NAME.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "PRODUCT ALIAS NAME can not be NULL EMPTY or UNKNOWN: " +
        identifier
      );
      return;
    }
    insuranceplan.alias[0] = inputMap.PRODUCT_ALIAS_NAME.trim();
  }

  /* Check OWNEDBY value is not null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.OWNEDBY.trim())) {
    Log.error(
      "Delimiter Found in OWNEDBY: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  OWNEDBY */
    let lvl5Array = inputMap.OWNEDBY.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.ownedBy.display = distinctArray[0];
    } else {
      insuranceplan.ownedBy.display = "Information Not Available";
    }
  } else {
    if (
      inputMap.OWNEDBY.trim().toUpperCase() != "NULL" &&
      inputMap.OWNEDBY.trim().toUpperCase() != "NAN" &&
      inputMap.OWNEDBY.trim() !== ""
    ) {
      insuranceplan.ownedBy.display = inputMap.OWNEDBY.trim();
    } else {
      insuranceplan.ownedBy.display = "Information Not Available";
    }
  }
  /* Check ADMINISTEREDBY value is not null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.ADMINISTEREDBY.trim())) {
    Log.error(
      "Delimiter Found in ADMINISTEREDBY: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  ADMINISTEREDBY */
    let lvl5Array = inputMap.ADMINISTEREDBY.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.administeredBy.display = distinctArray[0];
    } else {
      insuranceplan.administeredBy.display = "Information Not Available";
    }
  } else {
    if (
      inputMap.ADMINISTEREDBY.trim().toUpperCase() != "NULL" &&
      inputMap.ADMINISTEREDBY.trim().toUpperCase() != "NAN" &&
      inputMap.ADMINISTEREDBY.trim() !== ""
    ) {
      insuranceplan.administeredBy.display = inputMap.ADMINISTEREDBY.trim();
    } else {
      insuranceplan.administeredBy.display = "Information Not Available";
    }
  }
  /* Check COVERAGE_AREA value is not null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.COVERAGE_AREA.trim())) {
    Log.error(
      "Delimiter Found in COVERAGE_AREA: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  COVERAGE_AREA */
    let lvl5Array = inputMap.COVERAGE_AREA.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.coverageArea[0].display = inputMap.COVERAGE_AREA.trim();
    }
  } else {
    if (
      inputMap.COVERAGE_AREA.trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_AREA.trim().toUpperCase() != "NAN" &&
      inputMap.COVERAGE_AREA.trim() !== "" &&
      inputMap.COVERAGE_AREA.trim().toUpperCase() != "UNKNOWN"
    ) {
      insuranceplan.coverageArea[0].display = inputMap.COVERAGE_AREA.trim();
    }
  }

  /* Check COVERAGE_TYPE value is not null, empty, or unknown */
  if (rollUpDelimiter.test(inputMap.COVERAGE_TYPE.trim())) {
    Log.error(
      "Delimiter Found in COVERAGE_TYPE: " +
      identifier
    );

    /* Convert 5 level nested array into 1 level nested array  -  COVERAGE_TYPE */
    let lvl5Array = inputMap.COVERAGE_TYPE.split(lvl5Char);

    let lvl4Array = [];
    for (let id = 0; id < lvl5Array.length; id++) {
      lvl4Array.push(...lvl5Array[id].split(lvl4Char));
    }


    let lvl3Array = [];
    for (let id = 0; id < lvl4Array.length; id++) {
      lvl3Array.push(...lvl4Array[id].split(lvl3Char));
    }


    let lvl2Array = [];
    for (let id = 0; id < lvl3Array.length; id++) {
      lvl2Array.push(...lvl3Array[id].split(lvl2Char));
    }


    let lvl1Array = [];
    for (let id = 0; id < lvl2Array.length; id++) {
      lvl1Array.push(...lvl2Array[id].split(lvl1Char));
    }

    /* data filtering ,trimming and distinct */
    let distinctArray = lvl1Array
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== "" &&
          type.trim().toUpperCase() != "UNKNOWN"
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      insuranceplan.coverage[0].type.coding[0].display = distinctArray[0];
    }
  } else {
    if (
      inputMap.COVERAGE_TYPE.trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_TYPE.trim().toUpperCase() != "NAN" &&
      inputMap.COVERAGE_TYPE.trim() !== "" &&
      inputMap.COVERAGE_TYPE.trim().toUpperCase() != "UNKNOWN"
    ) {
      insuranceplan.coverage[0].type.coding[0].display = inputMap.COVERAGE_TYPE.trim();
    }
  }

  /* COVERAGE_BENEFIT_TYPE*/
  insuranceplan.coverage[0].benefit[0].type.text = "Not Available";

  var network_Arr_unique = inputMap.NETWORK.split(lvl5Char);
  var NETWORK_Unique = network_Arr_unique[0].split(lvl4Char);
  /* Populating network at plan level and creating distinct dictionary for transaction purposes. */

  for (let planlvl = 0; planlvl < network_Arr_unique.length; planlvl++) {
    network_Arr_unique_plan = network_Arr_unique[planlvl].split(lvl4Char);
    var setB = new Set(network_Arr_unique_plan);
    NETWORK_Unique = [...new Set(NETWORK_Unique)].filter((x) => setB.has(x));
  }

  /* Identifier */
  /* Level 5 */
  let PLAN_IDENTIFIER_Arr = [];
  if (
    inputMap.PLAN_IDENTIFIER.trim().toUpperCase() == "NULL" ||
    inputMap.PLAN_IDENTIFIER.trim().toUpperCase() == "NAN" ||
    inputMap.PLAN_IDENTIFIER.trim() === "" ||
    inputMap.PLAN_IDENTIFIER.trim().toUpperCase() == "UNKNOWN"
  ) {
    Log.error(
      "PLAN IDENTIFIER not valid: " +
      identifier
    );
  } else {
    PLAN_IDENTIFIER_Arr = inputMap.PLAN_IDENTIFIER.split(lvl5Char);
  }

  for (let lvl5 = 0; lvl5 < PLAN_IDENTIFIER_Arr.length; lvl5++) {
    if (
      PLAN_IDENTIFIER_Arr[lvl5].trim() != "NULL" &&
      PLAN_IDENTIFIER_Arr[lvl5].trim() !== "" &&
      PLAN_IDENTIFIER_Arr[lvl5].trim().toUpperCase() != "UNKNOWN"
    ) {
      insuranceplan.plan[lvl5].identifier[0].use = "official";
      insuranceplan.plan[lvl5].identifier[0].type.coding[0].system = "http://terminology.hl7.org/CodeSystem/v2-0203";
      insuranceplan.plan[lvl5].identifier[0].type.coding[0].code = "NIIP";
      insuranceplan.plan[lvl5].identifier[0].type.coding[0].display = "National Insurance Payor Identifier";
      insuranceplan.plan[lvl5].identifier[0].type.coding[0].userSelected = true;
      insuranceplan.plan[lvl5].identifier[0].type.text = "National Insurance Payor Identifier";
      insuranceplan.plan[lvl5].identifier[0].system = "http://system.com/INSURANCEPLAN-PLANID";
      insuranceplan.plan[lvl5].identifier[0].value = PLAN_IDENTIFIER_Arr[lvl5].trim();

      insuranceplan.plan[lvl5].type.coding[0].system = "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/InsurancePlanTypeCS";
      insuranceplan.plan[lvl5].type.coding[0].code = "gold";
      insuranceplan.plan[lvl5].type.coding[0].display = "Gold-QHP";

      /* Check PLAN_GENERALCOST_TYPE value is not NULL, empty or UNKNOWN */
      if (
        inputMap.PLAN_GENERALCOST_TYPE.trim() != "NULL" &&
        inputMap.PLAN_GENERALCOST_TYPE.trim() !== "" &&
        inputMap.PLAN_GENERALCOST_TYPE.trim().toUpperCase() != "UNKNOWN" &&
        inputMap.PLAN_GENERALCOST_COST.trim() != "NULL" &&
        inputMap.PLAN_GENERALCOST_COST.trim() !== "" &&
        inputMap.PLAN_GENERALCOST_COST.trim().toUpperCase() != "UNKNOWN"
      ) {
        insuranceplan.plan[lvl5].generalCost[0].type.coding[0].system =
          "http://terminology.hl7.org/CodeSystem/applicability";
        insuranceplan.plan[lvl5].generalCost[0].type.coding[0].code = "in-network";
        insuranceplan.plan[lvl5].generalCost[0].type.coding[0].display = inputMap.PLAN_GENERALCOST_TYPE.trim();

        insuranceplan.plan[0].generalCost[0].cost.value = Number(inputMap.PLAN_GENERALCOST_COST.trim());
      }

      /* Level 4 */
      var PLAN_SPECIFICCOST_CATEGORY_Arr_Outer =
        inputMap.PLAN_SPECIFICCOST_CATEGORY.split(lvl5Char)[lvl5].split(lvl4Char);

      for (let lvl4 = 0; lvl4 < PLAN_SPECIFICCOST_CATEGORY_Arr_Outer.length; lvl4++) {
        /* Level 3 */
        var PLAN_SPECIFICCOST_CATEGORY_Arr =
          PLAN_SPECIFICCOST_CATEGORY_Arr_Outer[lvl4].split(lvl3Char);
        for (let lvl3 = 0; lvl3 < PLAN_SPECIFICCOST_CATEGORY_Arr.length; lvl3++) {
          if (
            PLAN_SPECIFICCOST_CATEGORY_Arr[lvl3].trim() != "NULL" &&
            PLAN_SPECIFICCOST_CATEGORY_Arr[lvl3].trim() !== "" &&
            PLAN_SPECIFICCOST_CATEGORY_Arr[lvl3].trim().toUpperCase() != "UNKNOWN"
          ) {
            insuranceplan.plan[lvl5].specificCost[lvl3].category.coding[0].display =
              PLAN_SPECIFICCOST_CATEGORY_Arr[lvl3].trim();

            /* Level 2 */
            var PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr = inputMap.PLAN_SPECIFICCOST_BENEFIT_TYPE.split(
              lvl5Char
            )
            [lvl5].split(lvl4Char)
            [lvl4].split(lvl3Char)
            [lvl3].split(lvl2Char);

            for (let lvl2 = 0; lvl2 < PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr.length; lvl2++) {
              if (
                PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr[lvl2].trim() != "NULL" &&
                PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr[lvl2].trim() !== "" &&
                PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr[lvl2].trim().toUpperCase() != "UNKNOWN"
              ) {
                insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].type.coding[0].display =
                  PLAN_SPECIFICCOST_BENEFIT_TYPE_Arr[lvl2].trim();

                /* Level 1 */
                var PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr =
                  inputMap.PLAN_SPECIFICCOST_BENEFIT_COST_TYPE.split(lvl5Char)
                  [lvl5].split(lvl4Char)
                  [lvl4].split(lvl3Char)
                  [lvl3].split(lvl2Char)
                  [lvl2].split(lvl1Char);
                var PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY_Arr =
                  inputMap.PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY.split(lvl5Char)
                  [lvl5].split(lvl4Char)
                  [lvl4].split(lvl3Char)
                  [lvl3].split(lvl2Char)
                  [lvl2].split(lvl1Char);
                var PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIER_Arr =
                  inputMap.PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIERS.split(lvl5Char)
                  [lvl5].split(lvl4Char)
                  [lvl4].split(lvl3Char)
                  [lvl3].split(lvl2Char)
                  [lvl2].split(lvl1Char);
                var PLAN_SPECIFICCOST_BENEFIT_COST_VALUE_Arr =
                  inputMap.PLAN_SPECIFICCOST_BENEFIT_COST_VALUE.split(lvl5Char)
                  [lvl5].split(lvl4Char)
                  [lvl4].split(lvl3Char)
                  [lvl3].split(lvl2Char)
                  [lvl2].split(lvl1Char);
                for (let lvl1 = 0; lvl1 < PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr.length; lvl1++) {
                  if (
                    PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr[lvl1].trim() != "NULL" &&
                    PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr[lvl1].trim() !== "" &&
                    PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr[lvl1].trim().toUpperCase() != "UNKNOWN"
                  ) {
                    insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                      lvl1
                    ].type.coding[0].display = PLAN_SPECIFICCOST_BENEFIT_COST_TYPE_Arr[lvl1].trim();

                    if (
                      PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY_Arr[lvl1].trim() != "NULL" &&
                      PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY_Arr[lvl1].trim() !== "" &&
                      PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY_Arr[lvl1].trim().toUpperCase() !=
                      "UNKNOWN"
                    ) {
                      var applicability =
                        PLAN_SPECIFICCOST_BENEFIT_COST_APPLICABILITY_Arr[lvl1].trim();
                      switch (applicability) {
                        case "In Network":
                        case "Participating":
                        case "In Network and Participating":
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].system =
                            "http://terminology.hl7.org/CodeSystem/applicability";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].code = "in-network";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].display = "In Network";
                          break;
                        case "Out of Network":
                        case "Non-Participating":
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].system =
                            "http://terminology.hl7.org/CodeSystem/applicability";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].code = "out-of-network";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].display = "Out of Network";
                          break;
                        default:
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].system =
                            "http://terminology.hl7.org/CodeSystem/applicability";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].code = "other";
                          insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                            lvl1
                          ].applicability.coding[0].display = "Other";
                      }
                    }

                    if (
                      PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIER_Arr[lvl1].trim() != "NULL" &&
                      PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIER_Arr[lvl1].trim() !== "" &&
                      PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIER_Arr[lvl1].trim().toUpperCase() !=
                      "UNKNOWN"
                    ) {
                      insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                        lvl1
                      ].qualifiers[0].coding[0].display =
                        PLAN_SPECIFICCOST_BENEFIT_COST_QUALIFIER_Arr[lvl1].trim();

                      insuranceplan.plan[lvl5].specificCost[lvl3].benefit[lvl2].cost[
                        lvl1
                      ].value.value = PLAN_SPECIFICCOST_BENEFIT_COST_VALUE_Arr[lvl1].trim();
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /* Build a transaction and process it */
  var transaction = TransactionBuilder.newTransactionBuilder();

  var network_Arr = inputMap.NETWORK.split(lvl5Char);
  var NETWORK_Obj = {};
  /*populating network at plan and product level. */

  for (let planlvl = 0; planlvl < network_Arr.length; planlvl++) {
    network_Arr_plan = network_Arr[planlvl].split(lvl4Char);
    for (let index = 0; index < network_Arr_plan.length; index++) {
      if (
        !NETWORK_Obj.hasOwnProperty(network_Arr_plan[index].trim()) &&
        network_Arr_plan[index].trim() != "NULL" &&
        network_Arr_plan[index].trim() !== "" &&
        network_Arr_plan[index].trim().toUpperCase() != "UNKNOWN"
      ) {
        var organization = null;
        organization = ResourceBuilder.build("Organization");
        organization.id = Uuid.newPlaceholderId();

        tag = organization.meta.add("tag");
        tag.system = "http://system.com/TAG";
        tag.code = context.filename == null ? "POST" : context.filename;
        tag.display = "Data import location";

        organization.identifier[0].use = "official";
        organization.identifier[0].type.coding[0].system = "https://www.qhpcertification.cms.gov/s/QHP";
        organization.identifier[0].type.coding[0].code = "XX";
        organization.identifier[0].type.coding[0].display = "Organization Identifier";
        organization.identifier[0].type.coding[0].userSelected = true;
        organization.identifier[0].type.text = "Organization Identifier";
        organization.identifier[0].system = "http://system.com/ORGANIZATION";
        organization.identifier[0].value = network_Arr_plan[index].trim();

        /* can only populate networks common to all plans at insurancePlan level */
        if (NETWORK_Unique.includes(network_Arr_plan[index])) {
          let network = insuranceplan.add("network");
          network.reference = organization.id;
          network.display = network_Arr_plan[index].trim();
          let CD_Index = NETWORK_Unique.indexOf(network_Arr_plan[index]);
          NETWORK_Unique.splice(CD_Index, 1);
        }

        insuranceplan.plan[planlvl].network[index].reference = organization.id;
        insuranceplan.plan[planlvl].network[index].display = network_Arr_plan[index].trim();

        NETWORK_Obj[network_Arr_plan[index].trim()] = organization.id;

        transaction
          .createConditional(organization)
          .onToken(
            "identifier",
            "https://www.qhpcertification.cms.gov/s/QHP",
            network_Arr_plan[index].trim()
          );
      } else {
        if (
          network_Arr_plan[index].trim() != "NULL" &&
          network_Arr_plan[index].trim() !== "" &&
          network_Arr_plan[index].trim().toUpperCase() != "UNKNOWN"
        ) {
          insuranceplan.plan[planlvl].network[index].reference =
            NETWORK_Obj[network_Arr_plan[index].trim()];
          insuranceplan.plan[planlvl].network[index].display = network_Arr_plan[index].trim();
        }
      }
    }
  }

  // get an endpoint resource as a list & Id of the resource
  var endpointIdentifier = "123";
  var endpointList = Fhir.search()
    .forResource("Endpoint")
    .where("identifier", "|" + endpointIdentifier)
    .asList();
  if (endpointList.length > 0) {
    var endpoint = endpointList[0];
    insuranceplan.endpoint[0].reference = endpoint.id;
  } else {
    Log.error(
      "Endpoint " +
      endpointIdentifier +
      " doesn't exist: " +
      identifier
    );
  }

  transaction
    .updateConditional(insuranceplan)
    .onToken("identifier", insuranceplan.identifier[0].system, identifier);

  Fhir.withMaxConcurrencyRetry(10).transaction(transaction);
}
