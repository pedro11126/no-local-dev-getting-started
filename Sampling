"use strict";

/* globals logger, logmessages, config */
const errorHandler = require("../utils/errorHandler");
const getTransactionId = require("../utils/getTransactionId");

var {getConnection, getCredentials} = require("../../common/sf-action");


module.exports = async function (request, response, next) {
    const txid = getTransactionId(request);
    logger.startTransaction(txid);
    logger.debug(logmessages["WS1000"], { header: request.headers });

    var totalPerfHandler = logger.startPerformanceLog(logmessages["WS9000"]);

    const { body: { query, maxFetch, salesOrg } } = request;
    const message = { query, maxFetch, salesOrg };

    if (!isValidMessage(message)) {
        logger.error(logmessages["WS1002"], message);
        return errorHandler(logmessages["WS1002"].message, request, response, next, totalPerfHandler);
    }


    let sfConnection;
    try {
        sfConnection = await obtainSalesforceConnection();
    } catch (err) {
        logger.error(logmessages["WS9007"], {message: err});
        return errorHandler(err, request, response, next, totalPerfHandler);
    }


    logger.debug(logmessages["WS9002"], message.query);

    const records = [];
    let numRecords = 0;
 
    const queryMetadata = sfConnection.query(message.query)
    .on("record", record => {
        records.push(record);
        numRecords++;
    })
    .on("end", () => {
        logger.info(logmessages["WS9004"], {numNewRecords: numRecords});  
        if (queryMetadata.totalSize > config("SEGMENTATION_SAMPLING_MAX_TOTAL_SF_ROWS")) {
            logger.error(logmessages["WS9006"], {configuredMax: config("SEGMENTATION_SAMPLING_MAX_TOTAL_SF_ROWS"), totalSize: queryMetadata.totalSize});
            logger.endPerformanceLog(totalPerfHandler);
            logger.endTransaction();
            return response
                .status(400)
                .send({ status: 400, code: "WS9006", message: logmessages["WS9006"].message, records: records, totalRecords: queryMetadata.totalSize, maxConfiguredRecords: config("SEGMENTATION_SAMPLING_MAX_TOTAL_SF_ROWS") });
        }
        response.status(200).send({records: records, totalRecords: queryMetadata.totalSize});
        logger.debug(logmessages["WS1003"], message);
        logger.endPerformanceLog(totalPerfHandler);
        logger.endTransaction();      
    })
    .on("error", err => {
        logger.error(logmessages["WS9005"], err);
        return querySubmissionError(err, request, response, next, totalPerfHandler);
    })
    .run({ autoFetch: true, maxFetch: message.maxFetch });    
};


function isValidMessage(message) {
    let isValid = true;
    for (let prop in message) {
        if (!message[prop]) {
            isValid = false;
            break;
        }
    }
    return isValid;
}

async function obtainSalesforceConnection() {
    let credentials = await getCredentials();
    return await getConnection(credentials);
}

function querySubmissionError(err, req, res, next, perfhandler) {
    logger.endPerformanceLog(perfhandler);
    logger.endTransaction();
  
    res
    .status(400)
    .send({ status: 400, code: err.errorCode, message: err.message });
}
