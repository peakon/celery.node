"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArrays = (this && this.__spreadArrays) || function () {
    for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;
    for (var r = Array(s), k = 0, i = 0; i < il; i++)
        for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
            r[k] = a[j];
    return r;
};
Object.defineProperty(exports, "__esModule", { value: true });
var base_1 = require("./base");
var Worker = /** @class */ (function (_super) {
    __extends(Worker, _super);
    function Worker() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.handlers = {};
        _this.activeTasks = new Set();
        return _this;
    }
    /**
     * register task handler on worker handlers
     * @method Worker#register
     * @param {String} name the name of task for dispatching.
     * @param {Function} handler the function for task handling
     *
     * @example
     * worker.register('tasks.add', (a, b) => a + b);
     * worker.start();
     */
    Worker.prototype.register = function (name, handler) {
        if (!handler) {
            throw new Error("Undefined handler");
        }
        if (this.handlers[name]) {
            throw new Error("Already handler setted");
        }
        this.handlers[name] = function registHandler() {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            try {
                return Promise.resolve(handler.apply(void 0, args));
            }
            catch (err) {
                return Promise.reject(err);
            }
        };
    };
    /**
     * start celery worker to run
     * @method Worker#start
     * @example
     * worker.register('tasks.add', (a, b) => a + b);
     * worker.start();
     */
    Worker.prototype.start = function () {
        console.info("celery.node worker start...");
        console.info("registed task: " + Object.keys(this.handlers));
        return this.run().catch(function (err) { return console.error(err); });
    };
    /**
     * @method Worker#run
     * @private
     *
     * @returns {Promise}
     */
    Worker.prototype.run = function () {
        var _this = this;
        return this.isReady().then(function () { return _this.processTasks(); });
    };
    /**
     * @method Worker#processTasks
     * @private
     *
     * @returns function results
     */
    Worker.prototype.processTasks = function () {
        var consumer = this.getConsumer(this.conf.CELERY_QUEUE);
        return consumer();
    };
    /**
     * @method Worker#getConsumer
     * @private
     *
     * @param {String} queue queue name for task route
     */
    Worker.prototype.getConsumer = function (queue) {
        var _this = this;
        var onMessage = this.createTaskHandler();
        return function () { return _this.broker.subscribe(queue, onMessage); };
    };
    Worker.prototype.createTaskHandler = function () {
        var _this = this;
        var onTaskReceived = function (message) {
            if (!message) {
                return Promise.resolve();
            }
            var payload = null;
            var taskName = message.headers["task"];
            if (!taskName) {
                // protocol v1
                payload = message.decode();
                taskName = payload["task"];
            }
            // strategy
            var body;
            var headers;
            if (payload == null && !("args" in message.decode())) {
                body = message.decode(); // message.body;
                headers = message.headers;
            }
            else {
                var args_1 = payload["args"] || [];
                var kwargs_1 = payload["kwargs"] || {};
                var embed = {
                    callbacks: payload["callbacks"],
                    errbacks: payload["errbacks"],
                    chord: payload["chord"],
                    chain: null
                };
                body = [args_1, kwargs_1, embed];
                headers = {
                    lang: payload["lang"],
                    task: payload["task"],
                    id: payload["id"],
                    rootId: payload["root_id"],
                    parantId: payload["parentId"],
                    group: payload["group"],
                    meth: payload["meth"],
                    shadow: payload["shadow"],
                    eta: payload["eta"],
                    expires: payload["expires"],
                    retries: payload["retries"] || 0,
                    timelimit: payload["timelimit"] || [null, null],
                    kwargsrepr: payload["kwargsrepr"],
                    origin: payload["origin"]
                };
            }
            // request
            var args = body[0], kwargs = body[1] /*, embed */;
            var taskId = headers["id"];
            var handler = _this.handlers[taskName];
            if (!handler) {
                throw new Error("Missing process handler for task " + taskName);
            }
            console.info("celery.node Received task: " + taskName + "[" + taskId + "], args: " + args + ", kwargs: " + JSON.stringify(kwargs));
            var timeStart = process.hrtime();
            var taskPromise = handler.apply(void 0, __spreadArrays(args, [kwargs])).then(function (result) {
                var diff = process.hrtime(timeStart);
                console.info("celery.node Task " + taskName + "[" + taskId + "] succeeded in " + (diff[0] +
                    diff[1] / 1e9) + "s: " + result);
                _this.backend.storeResult(taskId, result, "SUCCESS");
                _this.activeTasks.delete(taskPromise);
            });
            // record the executing task
            _this.activeTasks.add(taskPromise);
            return taskPromise;
        };
        return onTaskReceived;
    };
    /**
     * @method Worker#whenCurrentJobsFinished
     *
     * @returns Promise that resolves when all jobs are finished
     */
    Worker.prototype.whenCurrentJobsFinished = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, Promise.all(Array.from(this.activeTasks))];
            });
        });
    };
    /**
     * @method Worker#stop
     *
     * @todo implement here
     */
    // eslint-disable-next-line class-methods-use-this
    Worker.prototype.stop = function () {
        throw new Error("not implemented yet");
    };
    return Worker;
}(base_1.default));
exports.default = Worker;
