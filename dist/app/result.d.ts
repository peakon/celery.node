import { CeleryBackend } from "../backends";
declare type AsyncResultStatus = "PENDING" | "SUCCESS" | "FAILURE" | "TIMEOUT";
export declare class AsyncResult {
    taskId: string;
    backend: CeleryBackend;
    status: AsyncResultStatus;
    result: any;
    /**
     * Asynchronous Result
     * @constructor AsyncResult
     * @param {string} taskId task id
     * @param {CeleryBackend} backend celery backend instance
     */
    constructor(taskId: string, backend: CeleryBackend);
    /**
     * @method AsyncResult#get
     * @returns {Promise}
     */
    get(timeout?: number): Promise<any>;
}
export {};
