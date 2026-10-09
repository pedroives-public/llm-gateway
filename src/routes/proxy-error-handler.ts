import type {
  FastifyError,
  FastifyReply,
  FastifyRequest,
  FastifySchemaValidationError,
} from "fastify";
import {
  emitReqRejected,
  type ReqRejectedReason,
} from "../observability/events.js";

// Error desk for Fastify-generated faults: schema rejection, body parsing,
// body limit and the unhandled exception. Upstream faults never arrive here;
// the route handler shapes them inline.

// Branch selection: a schema rejection requires BOTH the FST_ERR_VALIDATION
// code AND a populated `validation` array — a lone signal falls to the
// unhandled 500 branch. Parse failures match stable FST_ERR_CTP_* codes;
// explicit 413 is body-too-large; anything else is a gateway fault.
export function sendProxyError(
  error: FastifyError,
  request: FastifyRequest,
  reply: FastifyReply,
): void {
  if (
    error.validation &&
    error.validation.length > 0 &&
    error.code === "FST_ERR_VALIDATION"
  ) {
    const { code, reason } = deriveValidationRejection(error.validation);

    if (request.tenantId === null) {
      request.log.error(
        { req_id: request.reqId, err_name: error.name },
        "schema rejection with null tenant: auth misconfiguration",
      );
    } else {
      emitReqRejected(request.log, {
        req_id: request.reqId,
        tenant_id: request.tenantId,
        route: request.routeOptions.url,
        reason,
        status: 400,
      });
    }
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: error.message,
          type: "invalid_request_error",
          code,
        },
      });

    return;
  }

  if (error.code === "FST_ERR_CTP_INVALID_JSON_BODY") {
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body is not valid JSON",
          type: "invalid_request_error",
          code: "malformed_json",
        },
      });
    return;
  }

  if (error.code === "FST_ERR_CTP_EMPTY_JSON_BODY") {
    reply
      .code(400)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body is empty",
          type: "invalid_request_error",
          code: "empty_body",
        },
      });
    return;
  }

  if (error.code === "FST_ERR_CTP_INVALID_MEDIA_TYPE") {
    reply
      .code(415)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request content type unsupported",
          type: "invalid_request_error",
          code: "unsupported_content_type",
        },
      });
    return;
  }

  if (error.statusCode === 413) {
    reply
      .code(413)
      .header("x-gateway-error-class", "client-fault")
      .send({
        error: {
          message: "request body too large",
          type: "invalid_request_error",
          code: "request_too_large",
        },
      });
    return;
  }

  // Log only allowlisted fields — the raw error (stack/cause) must stay out of
  // both the client body and the log payload.
  request.log.error(
    { req_id: request.reqId, err_name: error.name },
    "unhandled exception in proxy handler",
  );
  reply
    .code(500)
    .header("x-gateway-error-class", "gateway-fault")
    .send({
      error: {
        message: "internal server error",
        type: "internal_error",
        code: "unhandled_exception",
      },
    });
}

function deriveValidationRejection(
  validation: FastifySchemaValidationError[],
): {
  code: string;
  reason: ReqRejectedReason;
} {
  // Both checks on purpose in each const branch: keyword alone would claim
  // any other `const` field; path alone would mislabel a type violation on
  // the same field (e.g. stream: "x").
  if (
    validation[0]?.keyword === "const" &&
    validation[0].instancePath === "/n"
  ) {
    return {
      code: "n_not_supported",
      reason: "cost_cap_exceeded",
    };
  }

  // Fires only with the streaming flag OFF: the flag-ON schema has no `const`
  // on `stream`. Remove it together with the flag-OFF form.
  if (
    validation[0]?.keyword === "const" &&
    validation[0].instancePath === "/stream"
  ) {
    return {
      code: "stream_not_supported",
      reason: "schema_validation",
    };
  }

  switch (validation[0]?.keyword) {
    case "required":
      return {
        code: `${String(validation[0].params.missingProperty)}_missing`,
        reason: "schema_validation",
      };
    case "minLength":
    case "minItems":
      return {
        code: `${validation[0].instancePath.slice(1)}_empty`,
        reason: "schema_validation",
      };
    case "maximum":
      return {
        code: `${validation[0].instancePath.slice(1)}_too_large`,
        reason: "cost_cap_exceeded",
      };
    default:
      return { code: "invalid_request", reason: "schema_validation" };
  }
}
