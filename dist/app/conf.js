"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var DEFAULT_CELERY_CONF = {
    CELERY_BROKER: "amqp://",
    CELERY_BROKER_OPTIONS: {},
    CELERY_BACKEND: "amqp://",
    CELERY_BACKEND_OPTIONS: {},
    CELERY_QUEUE: "celery",
    TASK_PROTOCOL: 2
};
function cloneObject(obj) {
    var clone = {};
    for (var i in obj) {
        if (typeof obj[i] == "object" && obj[i] != null)
            clone[i] = cloneObject(obj[i]);
        else
            clone[i] = obj[i];
    }
    return clone;
}
function defaultConf() {
    return cloneObject(DEFAULT_CELERY_CONF);
}
exports.defaultConf = defaultConf;
