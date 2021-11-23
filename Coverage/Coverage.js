/*
 * Coverage.js ETL Import CSV Mapping function - Expects input in the format:
 *
 * PATIENT_IDENTIFIER,PATIENT_PLAN_ID,PATIENT_PLAN_NAME,PATIENT_COVERAGE_ELIG_STARTDATE,
 * PATIENT_COVERAGE_ELIG_ENDDATE,PATIENT_MANAGING_ORGANIZATION,COVERAGE_TYPE,
 * COVERAGE_SUBSCRIBER_ID,COVERAGE_DEPENDENT_NUMBER,COVERAGE_RELATIONSHIP,COVERAGE_GROUP_ID,
 * COVERAGE_GROUP_NAME,COVERAGE_ORDER,COVERAGE_NETWORK,COVERAGE_CONTRACT,COVERAGE_IDENTIFIER
 *
 * Hashing Function: function hashEtlImportRow(inputMap,context){return inputMap.PATIENT_IDENTIFIER.trim();}
 */
/* Ticket #:     Assigned to:     Date Updated:

*/
function handleEtlImportRow(inputMap, context) {
  Log.info("Processing CSV row from file: " + context.filename);

  let lvl1Char = "[*]";
  let lvl2Char = "[=]";

  var rollUpDelimiter = new RegExp(/\[\*\]|\[\=\]/);

  if (
    inputMap.PATIENT_IDENTIFIER.trim().toUpperCase() == "NULL" ||
    inputMap.PATIENT_IDENTIFIER.trim() === "" ||
    inputMap.PATIENT_IDENTIFIER.trim().toUpperCase() == "UNKNOWN"
  ) {
    Log.error(
      "PATIENT_IDENTIFIER can not be UNKNOWN NULL or EMPTY: " +
      inputMap.PATIENT_IDENTIFIER.trim()
    );
    return;
  }
  if (rollUpDelimiter.test(inputMap.PATIENT_IDENTIFIER.trim())) {
    Log.error(
      "Delimiter Found in PATIENT_IDENTIFIER: " +
      inputMap.PATIENT_IDENTIFIER.trim()
    );
    return;
  }

  /* Lock on the Patient Id */
  context.lock(inputMap.PATIENT_IDENTIFIER.trim());
  let identifier = inputMap.PATIENT_IDENTIFIER.trim();

  var transaction = TransactionBuilder.newTransactionBuilder();
  /* Create a Coverage Resource */
  /* Multiple Coverages are made from a single Patient */

  var COV_IDENTIFIER_Arr = inputMap.COVERAGE_IDENTIFIER.trim().split(lvl2Char);
  for (let covIndex = 0; covIndex < COV_IDENTIFIER_Arr.length; covIndex++) {
    var coverage = null;
    coverage = ResourceBuilder.build("Coverage");
    coverage.id = Uuid.newPlaceholderId();

    tag = coverage.meta.add("tag");
    tag.system = "http://system.com/TAG";
    tag.code = context.filename == null ? "POST" : context.filename;
    tag.display = "Data import location";

    /* Profile */
    coverage.meta.profile[0] = "http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Coverage|1.1.0";
    coverage.meta.profile[1] = "http://hl7.org/fhir/us/davinci-hrex/StructureDefinition/hrex-coverage";

    /* Payor */
    coverage.payor[0].display = "Health Care Payor Name";

    /* Status*/
    let todayDate = new Date();
    let today = todayDate.toISOString().substr(0, 10);
    let newActiveEndDate = new Date(
      inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].replaceAll("/", "-").trim()
    );
    let activeEndDate = newActiveEndDate.toISOString().substr(0, 10);
    coverage.status = today <= activeEndDate ? "active" : "cancelled";

    /* Coverage Identifier */
    coverage.identifier[0].use = "official";
    coverage.identifier[0].type.coding[0].system = "http://terminology.hl7.org/CodeSystem/v2-0203";
    coverage.identifier[0].type.coding[0].code = "MB";
    coverage.identifier[0].type.coding[0].display = "Member Number";
    coverage.identifier[0].type.coding[0].userSelected = true;
    coverage.identifier[0].type.text = "Member Number";
    coverage.identifier[0].system = "http://system.com/COVERAGEID";
    coverage.identifier[0].value = COV_IDENTIFIER_Arr[covIndex].trim();

    /* Check PATIENT_COVERAGE_ELIG_STARTDATE value is not NULL, empty or UNKNOWN */
    if (
      inputMap.PATIENT_COVERAGE_ELIG_STARTDATE.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_COVERAGE_ELIG_STARTDATE.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.PATIENT_COVERAGE_ELIG_STARTDATE.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      let newDate = new Date(
        inputMap.PATIENT_COVERAGE_ELIG_STARTDATE.split(lvl2Char)[covIndex].replaceAll("/", "-").trim()
      );
      let startDate = newDate.toISOString().substr(0, 10);
      coverage.identifier[0].period.start = startDate; /* YYYY-MM-DD */
      coverage.period.start = startDate;
    }

    /* Check PATIENT_COVERAGE_ELIG_ENDDATE value is not NULL, empty or UNKNOWN */
    if (
      inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      if (
        new Date(inputMap.PATIENT_COVERAGE_ELIG_STARTDATE.split(lvl2Char)[covIndex].trim()).getTime() <=
        new Date(inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].trim()).getTime()
      ) {
        let newEndDate = new Date(
          inputMap.PATIENT_COVERAGE_ELIG_ENDDATE.split(lvl2Char)[covIndex].replaceAll("/", "-").trim()
        );
        let endDate = newEndDate.toISOString().substr(0, 10);
        coverage.period.end = endDate; /* YYYY-MM-DD */
        coverage.identifier[0].period.end = endDate;
      } else {
        coverage.identifier[0].period.end = "2199-12-31";
        coverage.period.end = "2199-12-31";
        Log.error(
          "elig period.end cannot be before period.start: " +
          identifier
        );
      }
    }

    /* Check PATIENT_MANAGING_ORGANIZATION value is not NULL, empty or UNKNOWN */
    if (rollUpDelimiter.test(inputMap.PATIENT_MANAGING_ORGANIZATION.trim())) {
      Log.error(
        "Delimiter Found in PATIENT_MANAGING_ORGANIZATION: " + identifier
      );

      /* Convert 2 level nested array into 1 level nested array  -  PATIENT_MANAGING_ORGANIZATION */
      let lvl2Array = inputMap.PATIENT_MANAGING_ORGANIZATION.split(lvl2Char);

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
        coverage.identifier[0].assigner.display = distinctArray[0];
      }
    } else {
      if (
        inputMap.PATIENT_MANAGING_ORGANIZATION.trim().toUpperCase() != "NULL" &&
        inputMap.PATIENT_MANAGING_ORGANIZATION.trim() !== "" &&
        inputMap.PATIENT_MANAGING_ORGANIZATION.trim().toUpperCase() != "UNKNOWN"
      ) {
        coverage.identifier[0].assigner.display = inputMap.PATIENT_MANAGING_ORGANIZATION.trim();
      }
    }


    /* Check COVERAGE_TYPE value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_TYPE.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_TYPE.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_TYPE.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.type.coding[0].system = "http://terminology.hl7.org/CodeSystem/v3-ActCode";

      var coverageType = inputMap.COVERAGE_TYPE.split(lvl2Char)[covIndex].trim().toUpperCase();
      switch (coverageType) {
        case "MEDICAL PRODUCT":
          coverage.type.coding[0].code = "MED";
          coverage.type.coding[0].display = "Medical";
          break;
        case "DENTAL PRODUCT":
          coverage.type.coding[0].code = "DNTL";
          coverage.type.coding[0].display = "Dental";
          break;
        case "VISION":
          coverage.type.coding[0].code = "VISPOL";
          coverage.type.coding[0].display = "vision care policy";
          break;
        default:
          coverage.type.coding[0].code = "HIP";
          coverage.type.coding[0].display = "health insurance plan policy";
          break;
      }

      coverage.type.text = coverage.type.coding[0].display;
    }
    let policyHolder = inputMap.COVERAGE_SUBSCRIBER_ID.split(lvl2Char)[covIndex].trim() + "00";
    let patient = inputMap.PATIENT_IDENTIFIER.trim();
    var subscriberList = Fhir.search()
      .forResource("Patient")
      .where("identifier", "|" + policyHolder)
      .asList();

    if (subscriberList.length == 1) {
      coverage.policyHolder.reference = subscriberList[0].id;
      /* Coverage Subscriber */
      coverage.subscriber.reference = subscriberList[0].id;
    } else if (subscriberList.length > 1) {
      /*Log error and reject*/
      Log.error(
        "Patient search found multiple references to Patient: " +
        identifier
      );
      return;
    } else {
      Log.error(
        "Patient search could not find reference to Patient: " +
        identifier
      );
      return;
    }

    coverage.subscriberId = inputMap.COVERAGE_SUBSCRIBER_ID.split(lvl2Char)[covIndex].trim();
    /* Beneficiary*/
    if (policyHolder == patient) {
      coverage.beneficiary.reference = subscriberList[0].id;
    } else {
      var beneficiaryList = Fhir.search()
        .forResource("Patient")
        .where("identifier", "|" + patient)
        .asList();
      if (beneficiaryList.length == 1) {
        coverage.beneficiary.reference = beneficiaryList[0].id;
      } else if (beneficiaryList.length > 1) {
        /*Log error and reject*/
        Log.error(
          "Patient search found multiple references to Patient: " +
          identifier
        );
        return;
      } else {
        Log.error(
          "Patient search could not find reference to Patient: " +
          identifier
        );
        return;
      }
    }

    /* Check COVERAGE_DEPENDENT_NUMBER value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_DEPENDENT_NUMBER.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_DEPENDENT_NUMBER.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_DEPENDENT_NUMBER.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.dependent = inputMap.COVERAGE_DEPENDENT_NUMBER.split(lvl2Char)[covIndex].trim();
    }

    /* Check COVERAGE_RELATIONSHIP value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_RELATIONSHIP.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_RELATIONSHIP.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_RELATIONSHIP.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.relationship.coding[0].system = "http://terminology.hl7.org/CodeSystem/subscriber-relationship";

      var coverageRelationship = inputMap.COVERAGE_RELATIONSHIP.split(lvl2Char)[covIndex].trim().toUpperCase();
      switch (coverageRelationship) {
        case "SUBSCRIBER":
          coverage.relationship.coding[0].code = "self";
          coverage.relationship.coding[0].display = "Self";
          break;
        case "DEPENDENT":
          coverage.relationship.coding[0].code = "child";
          coverage.relationship.coding[0].display = "Child";
          break;
        case "SPOUSE":
          coverage.relationship.coding[0].code = "spouse";
          coverage.relationship.coding[0].display = "Spouse";
          break;
        case "COMMON":
          coverage.relationship.coding[0].code = "common";
          coverage.relationship.coding[0].display = "Common";
          break;
        case "PARENT":
          coverage.relationship.coding[0].code = "parent";
          coverage.relationship.coding[0].display = "PARENT";
          break;
        default:
          coverage.relationship.coding[0].code = "other";
          coverage.relationship.coding[0].display = "Other";
          break;
      }

      coverage.relationship.text = coverage.relationship.coding[0].display;
    }

    /* Check COVERAGE_GROUP_ID value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_GROUP_ID.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_GROUP_ID.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_GROUP_ID.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN" &&
      inputMap.COVERAGE_GROUP_NAME.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_GROUP_NAME.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_GROUP_NAME.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      let groupAddition = coverage.add("class");
      groupAddition.type.coding[0].system = "http://terminology.hl7.org/CodeSystem/coverage-class";
      groupAddition.type.coding[0].code = "group";
      groupAddition.type.text = "An employee group";
      groupAddition.value = inputMap.COVERAGE_GROUP_ID.split(lvl2Char)[covIndex].trim();
      groupAddition.name = inputMap.COVERAGE_GROUP_NAME.split(lvl2Char)[covIndex].trim();
    }

    if (
      inputMap.PATIENT_PLAN_ID.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_PLAN_ID.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.PATIENT_PLAN_ID.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN" &&
      inputMap.PATIENT_PLAN_NAME.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.PATIENT_PLAN_NAME.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.PATIENT_PLAN_NAME.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      let planAddition = coverage.add("class");
      planAddition.type.coding[0].system = "http://terminology.hl7.org/CodeSystem/coverage-class";
      planAddition.type.coding[0].code = "plan";
      planAddition.type.text = "A specific suite of benefits.";
      planAddition.value = inputMap.PATIENT_PLAN_ID.split(lvl2Char)[covIndex].trim();
      planAddition.name = inputMap.PATIENT_PLAN_NAME.split(lvl2Char)[covIndex].trim();
    }

    /* Check COVERAGE_ORDER value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_ORDER.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_ORDER.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_ORDER.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.order = inputMap.COVERAGE_ORDER.split(lvl2Char)[covIndex].trim();
    }

    /* Check COVERAGE_NETWORK value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_NETWORK.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_NETWORK.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_NETWORK.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.network = inputMap.COVERAGE_NETWORK.split(lvl2Char)[covIndex].trim();
    }

    /* Check COVERAGE_CONTRACT value is not NULL, empty or UNKNOWN */
    if (
      inputMap.COVERAGE_CONTRACT.split(lvl2Char)[covIndex].trim().toUpperCase() != "NULL" &&
      inputMap.COVERAGE_CONTRACT.split(lvl2Char)[covIndex].trim() !== "" &&
      inputMap.COVERAGE_CONTRACT.split(lvl2Char)[covIndex].trim().toUpperCase() != "UNKNOWN"
    ) {
      coverage.contract[0].display = inputMap.COVERAGE_CONTRACT.split(lvl2Char)[covIndex].trim();
    }
    transaction.updateConditional(coverage).onToken("identifier", coverage.identifier[0].system, COV_IDENTIFIER_Arr[covIndex].trim());
  }

  Fhir.withMaxConcurrencyRetry(10).transaction(transaction);
}
