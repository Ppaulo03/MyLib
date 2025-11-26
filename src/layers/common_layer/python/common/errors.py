class AppError(Exception):
    """Classe base para erros da aplicação"""

    def __init__(self, message, status_code=500, error_code="INTERNAL_ERROR"):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class BadRequestError(AppError):
    def __init__(self, message, error_code="BAD_REQUEST"):
        super().__init__(message, 400, error_code)


class UnauthorizedError(AppError):
    def __init__(self, message="Unauthorized", error_code="UNAUTHORIZED"):
        super().__init__(message, 401, error_code)


class NotFoundError(AppError):
    def __init__(self, message="Resource not found", error_code="NOT_FOUND"):
        super().__init__(message, 404, error_code)
