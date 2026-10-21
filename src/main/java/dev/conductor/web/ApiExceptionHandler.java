package dev.conductor.web;

import java.util.*;
import org.slf4j.*;
import org.springframework.http.*;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
class ApiExceptionHandler {
    private static final Logger log=LoggerFactory.getLogger(ApiExceptionHandler.class);
    @ExceptionHandler(NoSuchElementException.class)
    ResponseEntity<String> notFound(NoSuchElementException e){return error(HttpStatus.NOT_FOUND,e.getMessage());}
    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<String> invalid(IllegalArgumentException e){return error(HttpStatus.BAD_REQUEST,Objects.toString(e.getMessage(),"Invalid request"));}
    @ExceptionHandler(UnsupportedOperationException.class)
    ResponseEntity<String> unsupported(UnsupportedOperationException e){return error(HttpStatus.CONFLICT,e.getMessage());}
    /** The client went away (for example a killed worker mid long-poll); there is no one to answer. */
    @ExceptionHandler(org.springframework.web.context.request.async.AsyncRequestNotUsableException.class)
    void disconnected() {}
    @ExceptionHandler(Exception.class)
    ResponseEntity<String> other(Exception e) {
        if(e instanceof ErrorResponse response)
            return error(response.getStatusCode(),Objects.toString(response.getBody().getDetail(),"Request failed"));
        log.error("Request failed",e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR,"Internal error; inspect server logs");
    }
    private static ResponseEntity<String> error(HttpStatusCode status,String message){return JsonHttp.json(status,Map.of("error",message));}
}
