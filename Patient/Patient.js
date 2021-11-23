/*
 * PATIENT.js ETL Import CSV Mapping function - Expects input in the format:
 *
 * PATIENT_IDENTIFIER,PATIENT_FULL_NAME,PATIENT_LAST_NAME,PATIENT_FIRST_NAME,PATIENT_MIDDLE_NAME,PATIENT_PREFIX,PATIENT_SUFFIX,
 * PATIENT_GENDER,PATIENT_BIRTHDATE,PATIENT_DECEASEDDATE,ADDRESS_LINE_1_TEXT,ADDRESS_LINE_1_TEXT,ADDRESS_LINE_3_TEXT,
 * CITY_NAME,STATE_NAME,POSTAL_CODE,COUNTY_NAME,PATIENT_ADDRESS_BEGINDATE,PATIENT_ADDRESS_ENDDATE,PATIENT_TELECOM,
 * PATIENT_RACE,PATIENT_ETHNICITY,PATIENT_LOB_NAME,COVERAGE_GROUP_ID,PATIENT_COVERAGE_ELIG_STARTDATE,
 * PATIENT_COVERAGE_ELIG_ENDDATE,PATIENT_MARITALSTATUS,PATIENT_LANGUAGE,PATIENT_PCP_FIRST_NAME,
 * PATIENT_PCP_MIDDLE_NAME,PATIENT_PCP_LAST_NAME,PATIENT_MANAGING_ORGANIZATION,PATIENT_LINK,PATIENT_LOB_ID
 *
 * Hashing Function: function hashEtlImportRow(inputMap,context){return inputMap.PATIENT_IDENTIFIER.trim();}
 */
/* Ticket #:     Assigned to:     Date Updated:

*/
function handleEtlImportRow(inputMap, context) {
  Log.info("Processing CSV row from file: " + context.filename);

  /* Create a patient */
  var patient = ResourceBuilder.build("Patient");

  let tag = patient.meta.add("tag");
  tag.system = "http://system.com/TAG";
  tag.code = context.filename == null ? "POST" : context.filename;
  tag.display = "Data import location";

  let lvl1Char = "[*]";

  var rollUpDelimiter = new RegExp(/\[\*\]/);

  /* Required PATIENT_IDENTIFIER value can not be null, empty, or unknown */
  if (
    inputMap.PATIENT_IDENTIFIER.trim().toUpperCase() == "NULL" ||
    inputMap.PATIENT_IDENTIFIER.trim() === "" ||
    inputMap.PATIENT_IDENTIFIER.trim().toUpperCase() == "UNKNOWN"
  ) {
    Log.error(
      "Patient identifier can not be null, empty, or unkown: " +
      inputMap.PATIENT_IDENTIFIER.trim()
    );
    return;
  }
  /* Mandatory columns can not contain delimiters */
  if (rollUpDelimiter.test(inputMap.PATIENT_IDENTIFIER.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_IDENTIFIER: " +
      inputMap.PATIENT_IDENTIFIER.trim()
    );
    return;
  }

  /* Lock on the Patient Identifier */
  context.lock(inputMap.PATIENT_IDENTIFIER.trim());
  patient.id = Uuid.newPlaceholderId();

  /* Identifiers */
  let identifier = inputMap.PATIENT_IDENTIFIER.trim();

  patient.identifier[0].use = "official";
  patient.identifier[0].type.coding[0].system = "http://terminology.hl7.org/CodeSystem/v2-0203";
  patient.identifier[0].type.coding[0].code = "MB";
  patient.identifier[0].type.coding[0].display = "Member Number";
  patient.identifier[0].type.coding[0].userSelected = true;
  patient.identifier[0].type.text = "Member Number";
  patient.identifier[0].system = "http://system.com/PATIENT";
  patient.identifier[0].value = identifier;

  patient.identifier[1].use = "official";
  patient.identifier[1].type.coding[0].system = "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBIdentifierType";
  patient.identifier[1].type.coding[0].code = "UM";
  patient.identifier[1].type.coding[0].display = "Unique Member ID";
  patient.identifier[1].type.coding[0].userSelected = true;
  patient.identifier[1].type.text = "Unique Member ID";
  patient.identifier[1].system = "http://system.com/UNIQUE";
  patient.identifier[1].value = identifier + "-" + inputMap.COVERAGE_GROUP_ID.trim();

  patient.identifier[2].use = "secondary";
  patient.identifier[2].type.text = "patient id & group id & lob id & lob name";
  patient.identifier[2].system = "http://system.com/PATIENT+GROUPID+LOBID+LOBNAME";
  patient.identifier[2].value = identifier + "-" + inputMap.COVERAGE_GROUP_ID.trim() + "-" + inputMap.PATIENT_LOB_NAME.trim() + "-" + inputMap.PATIENT_LOB_ID.trim();

  /* Profile */
  patient.meta.profile[0] = "http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Patient|1.1.0";
  patient.meta.profile[1] = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  /* Extension: Race */
  /* Required PATIENT_RACE value can not be NULL */
  var race = null;
  if (rollUpDelimiter.test(inputMap.PATIENT_RACE.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_RACE: " + identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_RACE.split(lvl1Char)
      .filter((type) => {
        return (
          type.trim().toUpperCase() != "NULL" &&
          type.trim().toUpperCase() != "NAN" &&
          type.trim() !== ""
        );
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if ((distinctArray.length > 0)) {
      race = distinctArray[0].toUpperCase();
    } else {
      Log.error(
        "MEMBER RACE can not be empty: " + identifier
      );
      return;
    }
  } else {
    if (inputMap.PATIENT_RACE.trim() == "NULL") {
      Log.error(
        "MEMBER RACE can not be NULL: " + identifier
      );
      return;
    }
    race = inputMap.PATIENT_RACE.trim().toUpperCase();
  }

  var extensionRace = patient.addExtension("http://hl7.org/fhir/us/core/StructureDefinition/us-core-race");

  var extensionRaceOMB = extensionRace.addExtension("ombCategory");
  extensionRaceOMB.valueCoding.system = "urn:oid:2.16.840.1.113883.6.238";

  switch (race) {
    case "AMERICAN INDIAN OR ALASKAN NAT":
      extensionRaceOMB.valueCoding.code = "1002-5";
      extensionRaceOMB.valueCoding.display = "American Indian or Alaska Native";
      break;
    case "ASIAN INDIAN":
    case "CAMBODIAN":
    case "CHINESE":
    case "FILIPINO":
    case "JAPANESE":
    case "KOREAN":
    case "LOATIAN":
    case "GUAMANIAN OR CHAMORRO":
    case "HMONG":
    case "VIETNAMESE":
    case "OTHER ASIAN":
      extensionRaceOMB.valueCoding.code = "2028-9";
      extensionRaceOMB.valueCoding.display = "Asian";
      break;
    case "BLACK OR AFRICAN AMERICAN":
      extensionRaceOMB.valueCoding.code = "2054-5";
      extensionRaceOMB.valueCoding.display = "Black or African American";
      break;
    case "NATIVE HAWAIIAN":
    case "OTHER PACIFIC ISLANDER":
    case "SAMOAN":
      extensionRaceOMB.valueCoding.code = "2076-8";
      extensionRaceOMB.valueCoding.display = "Native Hawaiian or Other Pacific Islander";
      break;
    case "WHITE":
      extensionRaceOMB.valueCoding.code = "2106-3";
      extensionRaceOMB.valueCoding.display = "White";
      break;
    case "2 OR MORE RACES":
    case "NOT ASSIGNED":
    case "OTHER RACE":
    case "UNKNOWN":
    case "":
      extensionRaceOMB.valueCoding.system = "http://terminology.hl7.org/CodeSystem/v3-NullFlavor";
      extensionRaceOMB.valueCoding.code = "UNK";
      extensionRaceOMB.valueCoding.display = "Unknown";
      break;
    case "DECLINED":
      extensionRaceOMB.valueCoding.system = "http://terminology.hl7.org/CodeSystem/v3-NullFlavor";
      extensionRaceOMB.valueCoding.code = "ASKU";
      extensionRaceOMB.valueCoding.display = "Asked but no answer";
      break;
    default:
      Log.error("MEMBER RACE could not be mapped: " + identifier);
      return;
  }

  var extensionRaceDisplay = extensionRace.addExtension("text");
  extensionRaceDisplay.valueString = extensionRaceOMB.valueCoding.display;

  /* Extension: Birthsex */
  /* Required PATIENT_BIRTHSEX value can not be NULL */
  var extensionBirthSexDisplay = patient.addExtension("http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex");
  extensionBirthSexDisplay.valueCode = "UNK";

  /* Extension: Ethnicity */
  /* Required PATIENT_ETHNICITY value can not be NULL */
  var ethnicity = null;
  if (rollUpDelimiter.test(inputMap.PATIENT_ETHNICITY.trim())) {
    Log.error(
      "Rollup Delimiter Found in PATIENT_ETHNICITY: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_ETHNICITY.split(lvl1Char)
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
      ethnicity = distinctArray[0].toUpperCase();
    } else {
      Log.error(
        "MEMBER ETHNICITY can not be NULL: " +
        identifier
      );
      return;
    }
  } else {
    if (inputMap.PATIENT_ETHNICITY.trim() == "NULL") {
      Log.error(
        "MEMBER ETHNICITY can not be NULL: " +
        identifier
      );
      return;
    }
    ethnicity = inputMap.PATIENT_ETHNICITY.trim().toUpperCase();
  }

  var extensionEthnicity = patient.addExtension("http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity");

  var extensionEthnicityOMB = extensionEthnicity.addExtension("ombCategory");
  extensionEthnicityOMB.valueCoding.system = "urn:oid:2.16.840.1.113883.6.238";

  switch (ethnicity) {
    case "CUBAN":
    case "GUATEMALAN":
    case "HISPANIC":
    case "LATINO":
    case "HISPANIC/LATINO":
    case "HISPANIC OR LATINO":
    case "MEXICAN, MEXICAN AMERICAN OR C":
    case "PUERTO RICAN":
    case "SALVADORIAN":
      extensionEthnicityOMB.valueCoding.code = "2135-2";
      extensionEthnicityOMB.valueCoding.display = "Hispanic or Latino";
      break;
    default:
      extensionEthnicityOMB.valueCoding.code = "2186-5";
      extensionEthnicityOMB.valueCoding.display = "Not Hispanic or Latino";
  }

  var extensionEthnicityDisplay = extensionEthnicity.addExtension("text");
  extensionEthnicityDisplay.valueString = extensionEthnicityOMB.valueCoding.display;

  /* Check PATIENT_MANAGING_ORGANIZATION value is not null, empty, or unkown */
  if (rollUpDelimiter.test(inputMap.PATIENT_MANAGING_ORGANIZATION.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_MANAGING_ORGANIZATION: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_MANAGING_ORGANIZATION.split(lvl1Char)
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
      patient.identifier[0].assigner.display = distinctArray[0];
      patient.managingOrganization.display = distinctArray[0];
    }
  } else {
    if (
      inputMap.PATIENT_MANAGING_ORGANIZATION.trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_MANAGING_ORGANIZATION.trim() !== "" &&
      inputMap.PATIENT_MANAGING_ORGANIZATION.trim().toUpperCase() != "UNKNOWN"
    ) {
      patient.identifier[0].assigner.display = inputMap.PATIENT_MANAGING_ORGANIZATION.trim();
      patient.managingOrganization.display = inputMap.PATIENT_MANAGING_ORGANIZATION.trim();
    }
  }

  /* Active */
  if (
    inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.trim().toUpperCase() == "NULL" ||
    inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.trim().toUpperCase() == "UNKNOWN" ||
    inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.trim() == ""
  ) {
    Log.error(
      "PATIENT_COVERAGE_ELIG_ENDDATE can not be null, empty, or unkown: " +
      identifier
    );
    return;
  }
  let today = new Date().toISOString().substr(0, 10);
  var activeValue = false;
  let dates_Arr = inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl1Char);
  for (let i = 0; i < dates_Arr.length; i++) {
    dates_Arr[i] = dates_Arr[i].replaceAll("/", "-");
    if (today <= dates_Arr[i].trim() && dates_Arr[i].trim().toUpperCase() != "NULL" && dates_Arr[i].trim().toUpperCase() != "UNKNOWN") {
      activeValue = true;
      break;
    }
  }
  patient.active = activeValue;

  /* Name */
  /* Required PATIENT_LAST_NAME value can not be null, empty, or unkown */
  var fullName = "";
  var firstName = "";
  var lastName = "";
  var middleName = "";
  var memberPrefix = "";
  var memberSuffix = "";
  if (rollUpDelimiter.test(inputMap.PATIENT_LAST_NAME.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_LAST_NAME: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_LAST_NAME.split(lvl1Char)
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
      lastName = distinctArray[0];
    } else {
      Log.error(
        "PATIENT_LAST_NAME can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PATIENT_LAST_NAME.trim().toUpperCase() == "NULL" ||
      inputMap.PATIENT_LAST_NAME.trim() === "" ||
      inputMap.PATIENT_LAST_NAME.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "PATIENT_LAST_NAME can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
    }
    lastName = inputMap.PATIENT_LAST_NAME.trim();
  }

  /* Required PATIENT_FIRST_NAME value can not be null, empty, or unkown */
  if (rollUpDelimiter.test(inputMap.PATIENT_FIRST_NAME.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_FIRST_NAME: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_FIRST_NAME.split(lvl1Char)
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
      firstName = distinctArray[0];
    } else {
      Log.error(
        "PATIENT_FIRST_NAME can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PATIENT_FIRST_NAME.trim().toUpperCase() == "NULL" ||
      inputMap.PATIENT_FIRST_NAME.trim() === "" ||
      inputMap.PATIENT_FIRST_NAME.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "PATIENT_FIRST_NAME can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
    }
    firstName = inputMap.PATIENT_FIRST_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.PATIENT_FULL_NAME.trim())) {
    Log.error(
      "Delimiter Found in  PATIENT_FULL_NAME: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_FULL_NAME.split(lvl1Char)
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
      fullName = distinctArray[0];
    }
  } else {
    fullName = inputMap.PATIENT_FULL_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.PATIENT_MIDDLE_NAME.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_MIDDLE_NAME" +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_MIDDLE_NAME.split(lvl1Char)
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
      middleName = distinctArray[0];
    }
  } else {
    middleName = inputMap.PATIENT_MIDDLE_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.PATIENT_PREFIX.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_PREFIX: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_PREFIX.split(lvl1Char)
      .filter((type) => {
        return type.trim().toUpperCase() != "NAN" && type.trim().toUpperCase() != "UNKNOWN";
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      distinctArray[0] != "NULL" ? (memberPrefix = distinctArray[0]) : (memberPrefix = "");
    }
  } else {
    inputMap.PATIENT_PREFIX.trim() != "NULL" ? (memberPrefix = inputMap.PATIENT_PREFIX.trim()) : (memberPrefix = "");
  }
  if (rollUpDelimiter.test(inputMap.PATIENT_SUFFIX.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_SUFFIX: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_SUFFIX.split(lvl1Char)
      .filter((type) => {
        return type.trim().toUpperCase() != "NAN" && type.trim().toUpperCase() != "UNKNOWN";
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      distinctArray[0] != "NULL" ? (memberSuffix = distinctArray[0]) : (memberSuffix = "");
    }
  } else {
    inputMap.PATIENT_SUFFIX.trim() != "NULL" ? (memberSuffix = inputMap.PATIENT_SUFFIX.trim()) : (memberSuffix = "");
  }

  patient.name[0].use = "official";
  patient.name[0].text = fullName;
  patient.name[0].family = lastName;
  patient.name[0].given[0] = firstName;
  patient.name[0].given[1] = middleName;
  patient.name[0].prefix[0] = memberPrefix;
  patient.name[0].suffix[0] = memberSuffix;

  /* Telecom */
  /* Check PATIENT_TELECOM value is not null, empty, or unkown */
  if (rollUpDelimiter.test(inputMap.PATIENT_TELECOM.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_TELECOM: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_TELECOM.split(lvl1Char)
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
      patient.telecom[0].system = "phone";
      patient.telecom[0].value = distinctArray[0];
      patient.telecom[0].use = "home";
    }
  } else {
    if (
      inputMap.PATIENT_TELECOM.trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_TELECOM.trim() !== "" &&
      inputMap.PATIENT_TELECOM.trim().toUpperCase() != "UNKNOWN"
    ) {
    }
    patient.telecom[0].system = "phone";
    patient.telecom[0].value = inputMap.PATIENT_TELECOM.trim();
    patient.telecom[0].use = "home";
  }

  /* Gender - male | female | other | unknown */
  var gender = null;
  if (rollUpDelimiter.test(inputMap.PATIENT_GENDER.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_GENDER: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_GENDER.split(lvl1Char)
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
      gender = distinctArray[0].toUpperCase();
    }
  } else {
    gender = inputMap.PATIENT_GENDER.trim().toUpperCase();
  }
  switch (gender) {
    case "MALE":
      patient.gender = "male";
      break;
    case "FEMALE":
      patient.gender = "female";
      break;
    case "OTHER":
      patient.gender = "other";
      break;
    case "":
    case "NULL":
    case "UNKNOWN":
      patient.gender = "unknown";
      break;
    default:
      Log.error("Patient GENDER could not be mapped: " + identifier);
      return;
  }

  /* DOB */
  /* Check PATIENT_BIRTHDATE value is not NULL, empty or UNKNOWN */
  if (rollUpDelimiter.test(inputMap.PATIENT_BIRTHDATE.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_BIRTHDATE: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_BIRTHDATE.split(lvl1Char)
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
    if (distinctArray.length > 1) {
      Log.error(
        "Patient BIRTHDATE can not have mulitple values: " +
        identifier
      );
      return;
    } else if ((distinctArray.length = 1)) {
      let newDate = new Date(distinctArray[0].replaceAll("/", "-"));
      let bday = newDate.toISOString().substr(0, 10);
      patient.birthDate = bday;
    } else {
      Log.error(
        "Patient BIRTHDATE can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
    }
  } else {
    if (
      inputMap.PATIENT_BIRTHDATE.trim().toUpperCase() == "NULL" ||
      inputMap.PATIENT_BIRTHDATE.trim() === "" ||
      inputMap.PATIENT_BIRTHDATE.trim().toUpperCase() == "UNKNOWN"
    ) {
      Log.error(
        "Patient BIRTHDATE can not be NULL EMPTY or UNKOWN: " +
        identifier
      );
      return;
      throw "PATIENT_BIRTHDATE is not valid";
    }
    let newDate = new Date(inputMap.PATIENT_BIRTHDATE.trim().replaceAll("/", "-"));
    let bday = newDate.toISOString().substr(0, 10);
    patient.birthDate = bday; /* YYYY-MM-DD */
  }

  var deceased = "";
  if (rollUpDelimiter.test(inputMap.PATIENT_DECEASEDDATE.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_DECEASEDDATE: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_DECEASEDDATE.split(lvl1Char)
      .filter((type) => {
        return type.trim().toUpperCase() != "NAN";
      })
      .map((elem) => elem.trim());

    distinctArray = [...new Set(distinctArray)];

    /*filtering first legit value as discussed */
    if (distinctArray.length > 0) {
      if (
        distinctArray[0].trim().toUpperCase() == "NULL" ||
        distinctArray[0].trim() === "" ||
        distinctArray[0].trim().toUpperCase() == "UNKNOWN"
      ) {
        patient.deceasedBoolean = false;
      } else {
        if (patient.active) {
          patient.deceasedBoolean = false;
        } else {
          let newDate = new Date(distinctArray[0].replaceAll("/", "-").trim());
          let deadDate = newDate.toISOString().substr(0, 10);
          patient.deceasedDateTime = deadDate;
        }
      }
    }
  } else {
    if (
      inputMap.PATIENT_DECEASEDDATE.trim().toUpperCase() == "NULL" ||
      inputMap.PATIENT_DECEASEDDATE.trim() === "" ||
      inputMap.PATIENT_DECEASEDDATE.trim().toUpperCase() == "UNKNOWN"
    ) {
      patient.deceasedBoolean = false;
    } else {
      if (patient.active) {
        patient.deceasedBoolean = false;
      } else {
        let newDate = new Date(inputMap.PATIENT_DECEASEDDATE.replaceAll("/", "-").trim());
        let deadDate = newDate.toISOString().substr(0, 10);
        patient.deceasedDateTime = deadDate; /* YYYY-MM-DD */
      }
    }
  }

  /* Address */
  var line1 = "";
  var line2 = "";
  var line3 = "";
  var cityName = "";
  var countyName = "";
  var stateName = "";
  var postalCode = "";
  var addressStart = "";
  var addressEnd = "";

  if (rollUpDelimiter.test(inputMap.ADDRESS_LINE_1_TEXT.trim())) {
    Log.error(
      "Delimiter Found in ADDRESS_LINE_1_TEXT: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.ADDRESS_LINE_1_TEXT.split(lvl1Char)
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
      line1 = distinctArray[0];
    } else {
      line1 = "NULL";
    }
  } else {
    line1 = inputMap.ADDRESS_LINE_1_TEXT.trim();
  }

  if (rollUpDelimiter.test(inputMap.ADDRESS_LINE_2_TEXT.trim())) {
    Log.error(
      "Delimiter Found in ADDRESS_LINE_2_TEXT: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.ADDRESS_LINE_2_TEXT.split(lvl1Char)
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
      line2 = distinctArray[0];
    } else {
      line2 = "NULL";
    }
  } else {
    if (inputMap.ADDRESS_LINE_2_TEXT.trim() != "NULL") {
      line2 = inputMap.ADDRESS_LINE_2_TEXT.trim();
    } else {
      line2 = "NULL";
    }
  }

  if (rollUpDelimiter.test(inputMap.ADDRESS_LINE_3_TEXT.trim())) {
    Log.error(
      "Delimiter Found in ADDRESS_LINE_3_TEXT: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.ADDRESS_LINE_3_TEXT.split(lvl1Char)
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
      line3 = distinctArray[0];
    } else {
      line3 = "NULL";
    }
  } else {
    if (inputMap.ADDRESS_LINE_3_TEXT.trim() != "NULL") {
      line3 = inputMap.ADDRESS_LINE_3_TEXT.trim();
    } else {
      line3 = "NULL";
    }
  }

  if (rollUpDelimiter.test(inputMap.CITY_NAME.trim())) {
    Log.error(
      "Delimiter Found in CITY_NAME: " + identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.CITY_NAME.split(lvl1Char)
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
      cityName = distinctArray[0];
    }
  } else {
    cityName = inputMap.CITY_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.COUNTY_NAME.trim())) {
    Log.error(
      context.filename +
      "|COUNTY_NAME|ROLLUP|" +
      inputMap.COUNTY_NAME.trim() +
      "Delimiter Found in COUNTY_NAME: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.COUNTY_NAME.split(lvl1Char)
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
      countyName = distinctArray[0];
    }
  } else {
    countyName = inputMap.COUNTY_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.STATE_NAME.trim())) {
    Log.error(
      "Delimiter Found in STATE_NAME: " + identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.STATE_NAME.split(lvl1Char)
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
      stateName = distinctArray[0];
    }
  } else {
    stateName = inputMap.STATE_NAME.trim();
  }

  if (rollUpDelimiter.test(inputMap.POSTAL_CODE.trim())) {
    Log.error(
      "Delimiter Found in POSTAL_CODE: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.POSTAL_CODE.split(lvl1Char)
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
      postalCode = distinctArray[0];
    }
  } else {
    postalCode = inputMap.POSTAL_CODE.trim();
  }

  if (
    inputMap.PATIENT_ADDRESS_BEGINDATE.trim() != "" &&
    inputMap.PATIENT_ADDRESS_BEGINDATE.toUpperCase().trim() != "NULL" &&
    inputMap.PATIENT_ADDRESS_BEGINDATE.toUpperCase().trim() != "UNKNOWN" &&
    inputMap.PATIENT_ADDRESS_ENDDATE.trim() != "" &&
    inputMap.PATIENT_ADDRESS_ENDDATE.toUpperCase().trim() != "NULL" &&
    inputMap.PATIENT_ADDRESS_ENDDATE.toUpperCase().trim() != "UNKNOWN"
  ) {
    if (rollUpDelimiter.test(inputMap.PATIENT_ADDRESS_BEGINDATE.trim())) {
      Log.error(
        "Delimiter Found in PATIENT_ADDRESS_BEGINDATE: " +
        identifier
      );

      /* data filtering ,trimming and distinct */
      let distinctArray = inputMap.PATIENT_ADDRESS_BEGINDATE.split(lvl1Char)
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
        let newDate = new Date(distinctArray[0].replaceAll("/", "-").trim());
        let beginDate = newDate.toISOString().substr(0, 10);
        addressStart = beginDate;
      }
    } else {
      let newDate = new Date(inputMap.PATIENT_ADDRESS_BEGINDATE.replaceAll("/", "-").trim());
      let beginDate = newDate.toISOString().substr(0, 10);
      addressStart = beginDate;
    }

    if (rollUpDelimiter.test(inputMap.PATIENT_ADDRESS_ENDDATE.trim())) {
      Log.error(
        "Delimiter Found in PATIENT_ADDRESS_ENDDATE: " +
        identifier
      );

      /* data filtering ,trimming and distinct */
      let distinctArray = inputMap.PATIENT_ADDRESS_ENDDATE.split(lvl1Char)
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
        let newDate = new Date(distinctArray[0].replaceAll("/", "-").trim());
        let endDate = newDate.toISOString().substr(0, 10);
        addressEnd = endDate;
      }
    } else {
      let newDate = new Date(inputMap.PATIENT_ADDRESS_ENDDATE.replaceAll("/", "-").trim());
      let endDate = newDate.toISOString().substr(0, 10);
      addressEnd = endDate;
    }

    if (addressStart <= addressEnd) {
      patient.address[0].period.start = addressStart;
      patient.address[0].period.end = addressEnd;
    } else {
      patient.address[0].period.start = addressStart;
      patient.address[0].period.end = "2199-12-31";
      Log.error(
        "Address period.end cannot be before period.start: " +
        identifier
      );
    }
  } else {
    Log.error(
      "Address NULL, Empty and Unknown were found: " +
      identifier
    );
  }
  if (line1 != "" && cityName != "" && stateName != "" && postalCode != "") {
    patient.address[0].use = "home";
    patient.address[0].type = "both";
    patient.address[0].text = line1 + ", " + cityName + ", " + stateName + " " + postalCode;
  }

  if (line1 != "NULL") {
    patient.address[0].line[0] = line1;
    if (line2 != "NULL") {
      patient.address[0].line[1] = line2;
      if (line3 != "NULL") {
        patient.address[0].line[2] = line3;
      }
    }
  }

  patient.address[0].city = cityName;
  patient.address[0].district = countyName;
  patient.address[0].state = stateName;
  patient.address[0].postalCode = postalCode;

  patient.maritalStatus.coding[0].system = "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus";
  patient.maritalStatus.coding[0].userSelected = true;

  /* Marital Status */
  var maritalStatus = null;
  if (rollUpDelimiter.test(inputMap.PATIENT_MARITALSTATUS.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_MARITALSTATUS: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_MARITALSTATUS.split(lvl1Char)
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
      maritalStatus = distinctArray[0].toUpperCase();
    }
  } else {
    maritalStatus = inputMap.PATIENT_MARITALSTATUS.trim().toUpperCase();
  }
  switch (maritalStatus) {
    case "SEPARATED":
      patient.maritalStatus.coding[0].code = "L";
      patient.maritalStatus.coding[0].display = "Legally Separated";
      break;
    case "PARTNER":
      patient.maritalStatus.coding[0].code = "T";
      patient.maritalStatus.coding[0].display = "Domestic partner";
      break;
    case "DIVORCED":
      patient.maritalStatus.coding[0].code = "D";
      patient.maritalStatus.coding[0].display = "Divorced";
      break;
    case "SINGLE":
      patient.maritalStatus.coding[0].code = "S";
      patient.maritalStatus.coding[0].display = "Never Married";
      break;
    case "DIVORCED":
      patient.maritalStatus.coding[0].code = "D";
      patient.maritalStatus.coding[0].display = "Divorced";
      break;
    case "WIDOWED":
      patient.maritalStatus.coding[0].code = "W";
      patient.maritalStatus.coding[0].display = "Widowed";
      break;
    case "MARRIED":
      patient.maritalStatus.coding[0].code = "M";
      patient.maritalStatus.coding[0].display = "Married";
      break;
    case "UNMARRIED":
      patient.maritalStatus.coding[0].code = "U";
      patient.maritalStatus.coding[0].display = "unmarried";
      break;
    case "":
    case null:
    case "NULL":
    case "UNDEFINED":
    case "UNKNOWN":
    case "UNREPORTED":
      patient.maritalStatus.coding[0].system = "http://terminology.hl7.org/CodeSystem/v3-NullFlavor";
      patient.maritalStatus.coding[0].code = "UNK";
      patient.maritalStatus.coding[0].display = "unknown";
      break;
    default:
      Log.error(
        context.filename + "|PATIENT_MARITALSTATUS|MAPPING|" + maritalStatus + "|marital status could not be mapped|WARN|" + identifier
      );
  }

  patient.maritalStatus.text = patient.maritalStatus.coding[0].display;

  /* Communication */
  /* Extensible binding - system is not required */
  patient.communication[0].language.coding[0].system = "urn:ietf:bcp:47";
  patient.communication[0].language.coding[0].userSelected = true;
  patient.communication[0].preferred = true;

  var communication = null;
  if (rollUpDelimiter.test(inputMap.PATIENT_LANGUAGE.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_LANGUAGE: " +
      identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_LANGUAGE.split(lvl1Char)
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
      communication = distinctArray[0];
    }
  } else {
    communication = inputMap.PATIENT_LANGUAGE.trim();
  }
  switch (communication) {
    case "Arabic":
    case "ARABIC":
      patient.communication[0].language.coding[0].code = "ar";
      patient.communication[0].language.coding[0].display = "Arabic";
      break;
    case "BENGALI":
      patient.communication[0].language.coding[0].code = "bn";
      patient.communication[0].language.coding[0].display = "Bengali";
      break;
    case "CZECH":
    case "CZECKOSLOVAKIAN":
      patient.communication[0].language.coding[0].code = "cs";
      patient.communication[0].language.coding[0].display = "Czech";
      break;
    case "DANISH":
      patient.communication[0].language.coding[0].code = "da";
      patient.communication[0].language.coding[0].display = "Danish";
      break;
    case "English":
    case "ENGLISH":
      patient.communication[0].language.coding[0].code = "en";
      patient.communication[0].language.coding[0].display = "English";
      break;
    case "GERMAN":
      patient.communication[0].language.coding[0].code = "de";
      patient.communication[0].language.coding[0].display = "German";
      break;
    case "GREEK":
      patient.communication[0].language.coding[0].code = "el";
      patient.communication[0].language.coding[0].display = "Greek";
      break;
    case "Spanish":
    case "SPANISH":
      patient.communication[0].language.coding[0].code = "es";
      patient.communication[0].language.coding[0].display = "Spanish";
      break;
    case "FINNISH":
      patient.communication[0].language.coding[0].code = "fi";
      patient.communication[0].language.coding[0].display = "Finnish";
      break;
    case "FRENCH":
      patient.communication[0].language.coding[0].code = "fr";
      patient.communication[0].language.coding[0].display = "French";
      break;
    case "FRISIAN":
      patient.communication[0].language.coding[0].code = "fy";
      patient.communication[0].language.coding[0].display = "Frysian";
      break;
    case "HINDI":
      patient.communication[0].language.coding[0].code = "hi";
      patient.communication[0].language.coding[0].display = "Hindi";
      break;
    case "CROATIAN":
      patient.communication[0].language.coding[0].code = "hr";
      patient.communication[0].language.coding[0].display = "Croatian";
      break;
    case "ITALIAN":
      patient.communication[0].language.coding[0].code = "it";
      patient.communication[0].language.coding[0].display = "Italian";
      break;
    case "JAPANESE":
      patient.communication[0].language.coding[0].code = "ja";
      patient.communication[0].language.coding[0].display = "Japanese";
      break;
    case "Korean":
    case "KOREAN":
      patient.communication[0].language.coding[0].code = "ko";
      patient.communication[0].language.coding[0].display = "Korean";
      break;
    case "DUTCH":
      patient.communication[0].language.coding[0].code = "nl";
      patient.communication[0].language.coding[0].display = "Dutch";
      break;
    case "NORWEGIAN":
      patient.communication[0].language.coding[0].code = "no";
      patient.communication[0].language.coding[0].display = "Norwegian";
      break;
    case "PUNJABI":
      patient.communication[0].language.coding[0].code = "pa";
      patient.communication[0].language.coding[0].display = "Punjabi";
      break;
    case "POLISH":
      patient.communication[0].language.coding[0].code = "pa";
      patient.communication[0].language.coding[0].display = "Polish";
      break;
    case "PORTUGUESE":
      patient.communication[0].language.coding[0].code = "pt";
      patient.communication[0].language.coding[0].display = "Portuguese";
      break;
    case "Russian":
    case "RUSSIAN":
      patient.communication[0].language.coding[0].code = "ru";
      patient.communication[0].language.coding[0].display = "Russian";
      break;
    case "SERBIAN":
      patient.communication[0].language.coding[0].code = "sr";
      patient.communication[0].language.coding[0].display = "Serbian";
      break;
    case "SWEDISH":
      patient.communication[0].language.coding[0].code = "sv";
      patient.communication[0].language.coding[0].display = "Swedish";
      break;
    case "TELEGU":
      patient.communication[0].language.coding[0].code = "te";
      patient.communication[0].language.coding[0].display = "Telegu";
      break;
    case "CHINESE":
    case "CHINESE (FAMILY)":
      patient.communication[0].language.coding[0].code = "zh";
      patient.communication[0].language.coding[0].display = "Chinese";
      break;
    case null:
    case "":
    case "NULL":
    case "UNKNOWN":
    case "NO RESPONSE, CLIENT DECLINED TO STATE":
    case "NO VALID DATA":
    case "NO VALID DATA REPORTED (MEDS GENERATED)":
    case "NOT ASSIGNED":
    case "NOT AVAILABLE":
    case "NOT SELECTED":
      patient.communication[0].language.coding[0].code = "en"; /* Default to English */
      patient.communication[0].language.coding[0].display = "English";
      break;
    default:
      patient.communication[0].language.coding[0].code = "en"; /* Default to English */
      patient.communication[0].language.coding[0].display = "English";
      break;
  }

  patient.communication[0].language.text = patient.communication[0].language.coding[0].display;

  /* General Practitioner */
  /* Check PATIENT_PCP_FIRST_NAME and PATIENT_PCP_LAST_NAME value is not NULL, empty or UNKNOWN */
  if (
    inputMap.PATIENT_PCP_FIRST_NAME.trim().toUpperCase() != "NULL" &&
    inputMap.PATIENT_PCP_FIRST_NAME.trim() !== "" &&
    inputMap.PATIENT_PCP_FIRST_NAME.trim().toUpperCase() != "UNKNOWN" &&
    inputMap.PATIENT_PCP_LAST_NAME.trim().toUpperCase() != "NULL" &&
    inputMap.PATIENT_PCP_LAST_NAME.trim() !== "" &&
    inputMap.PATIENT_PCP_LAST_NAME.trim().toUpperCase() != "UNKNOWN"
  ) {
    let pcp_first_Arr = inputMap.PATIENT_PCP_FIRST_NAME.split(lvl1Char);
    let pcp_last_Arr = inputMap.PATIENT_PCP_LAST_NAME.split(lvl1Char);
    let pcp_middle_Arr = inputMap.PATIENT_PCP_MIDDLE_NAME.split(lvl1Char);

    if (pcp_first_Arr.length !== pcp_last_Arr.length) {
      Log.error(
        "|PCP first names and last names counts don't match: " +
        identifier
      );
    } else {
      for (let i = 0; i < pcp_first_Arr.length; i++) {
        if (
          pcp_first_Arr[i].trim().toUpperCase() != "NULL" &&
          pcp_first_Arr[i].trim() !== "" &&
          pcp_first_Arr[i].trim().toUpperCase() != "UNKNOWN" &&
          pcp_last_Arr[i].trim().toUpperCase() != "NULL" &&
          pcp_last_Arr[i].trim() !== "" &&
          pcp_last_Arr[i].trim().toUpperCase() != "UNKNOWN"
        ) {
          let pcp = patient.add("generalPractitioner");
          pcp.display =
            pcp_first_Arr[i].trim() +
            (pcp_middle_Arr[i].trim() == "" ? " " : " " + pcp_middle_Arr[i].trim() + " ") +
            pcp_last_Arr[i].trim();
        }
      }
    }

  }

  /* Link */
  /* Check PATIENT_LINK value is not NULL, empty or UNKNOWN */
  if (rollUpDelimiter.test(inputMap.PATIENT_LINK.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_LINK: " + identifier
    );

    /* data filtering ,trimming and distinct */
    let distinctArray = inputMap.PATIENT_LINK.split(lvl1Char)
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
      patient.link[0].other.display = distinctArray[0];
      patient.link[0].type = "refer";
    }
  } else {
    if (
      inputMap.PATIENT_LINK.trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_LINK.trim() !== "" &&
      inputMap.PATIENT_LINK.trim().toUpperCase() != "UNKNOWN"
    ) {
      patient.link[0].other.display = inputMap.PATIENT_LINK.trim();
      patient.link[0].type = "refer";
    }
  }

  /*Create a transaction to persist data*/
  var transaction = TransactionBuilder.newTransactionBuilder();

  transaction.updateConditional(patient).onToken("identifier", patient.identifier[0].system, identifier);

  Fhir.withMaxConcurrencyRetry(10).transaction(transaction);
}
