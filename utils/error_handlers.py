"""
Centralized error handling for Flask API
"""
import logging
from flask import jsonify
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError, DatabaseError

logger = logging.getLogger(__name__)


def register_error_handlers(app):
    """Register all error handlers with Flask app"""

    @app.errorhandler(ValueError)
    def handle_value_error(error):
        """Handle validation errors"""
        logger.warning(f"Validation error: {error}")
        return jsonify({
            'error': 'Validation Error',
            'message': str(error),
            'status': 400
        }), 400

    @app.errorhandler(IntegrityError)
    def handle_integrity_error(error):
        """Handle database integrity errors (unique constraints, etc)"""
        logger.error(f"Database integrity error: {error}", exc_info=True)

        # Extract meaningful message
        error_msg = str(error.orig) if hasattr(error, 'orig') else str(error)

        if 'unique constraint' in error_msg.lower():
            message = "A record with this information already exists"
        elif 'foreign key constraint' in error_msg.lower():
            message = "Referenced record does not exist"
        else:
            message = "Database constraint violation"

        return jsonify({
            'error': 'Database Integrity Error',
            'message': message,
            'status': 409
        }), 409

    @app.errorhandler(DatabaseError)
    def handle_database_error(error):
        """Handle general database errors"""
        logger.error(f"Database error: {error}", exc_info=True)
        return jsonify({
            'error': 'Database Error',
            'message': 'An error occurred while accessing the database',
            'status': 500
        }), 500

    @app.errorhandler(HTTPException)
    def handle_http_exception(error):
        """Handle HTTP exceptions (404, 403, etc)"""
        logger.info(f"HTTP exception: {error}")
        return jsonify({
            'error': error.name,
            'message': error.description,
            'status': error.code
        }), error.code

    @app.errorhandler(404)
    def handle_not_found(error):
        """Handle 404 errors"""
        return jsonify({
            'error': 'Not Found',
            'message': 'The requested resource was not found',
            'status': 404
        }), 404

    @app.errorhandler(500)
    def handle_internal_error(error):
        """Handle 500 errors"""
        logger.error(f"Internal server error: {error}", exc_info=True)
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred',
            'status': 500
        }), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        """Catch-all handler for unexpected errors"""
        logger.error(f"Unexpected error: {error}", exc_info=True)
        return jsonify({
            'error': 'Unexpected Error',
            'message': 'An unexpected error occurred. Please contact support.',
            'status': 500
        }), 500


class APIError(Exception):
    """Custom API error class"""

    def __init__(self, message, status_code=400, payload=None):
        super().__init__()
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        rv['status'] = self.status_code
        rv['error'] = self.__class__.__name__
        return rv


def register_api_error_handler(app):
    """Register custom API error handler"""

    @app.errorhandler(APIError)
    def handle_api_error(error):
        logger.warning(f"API error: {error.message}")
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response
