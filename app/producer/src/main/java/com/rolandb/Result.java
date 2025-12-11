package com.rolandb;

/**
 * A small utility type that encapsulates either an error or a value.
 * 
 * @param <T>
 *            The type of value returned on success.
 */
public sealed abstract class Result<T> permits Result.Ok, Result.Err {
    private Result() {
    }

    /**
     * Return whether this result represents an error.
     * 
     * @return {@code true} if this is an error, {@code false} otherwise.
     */
    public abstract boolean isError();

    /**
     * Get the value stored in this result.
     * 
     * @return The value in this result or {@code null} if this is an error.
     */
    public abstract T getValue();

    /**
     * Get the error stored in this result.
     * 
     * @return The error in this result or {@code null} if this is not an error.
     */
    public abstract Throwable getError();

    /**
     * Build a new result instance with given value.
     * 
     * @param <T>
     *            Type of result value.
     * @param value
     *            The value of the result.
     * @return The result.
     */
    public static <T> Result<T> ok(T value) {
        return new Ok<>(value);
    }

    /**
     * Build a new result instance with given error.
     * 
     * @param <T>
     *            Type of result value.
     * @param error
     *            The error of the result.
     * @return The result.
     */
    public static <T> Result<T> err(Throwable error) {
        return new Err<>(error);
    }

    /**
     * The class representing a successful, non-error, result.
     * 
     * @param <T>
     *            The type of value returned on success.
     */
    public static final class Ok<T> extends Result<T> {
        private final T value;

        /**
         * Create a new non error result with the given value.
         * 
         * @param value
         *            The value of the result.
         */
        public Ok(T value) {
            this.value = value;
        }

        @Override
        public boolean isError() {
            return false;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public Throwable getError() {
            return null;
        }
    }

    /**
     * The class representing a result with error.
     * 
     * @param <T>
     *            The type of value returned on success.
     */
    public static final class Err<T> extends Result<T> {
        private final Throwable error;

        /**
         * Create a new error result.
         * 
         * @param error
         *            The value of the error.
         */
        public Err(Throwable error) {
            this.error = error;
        }

        @Override
        public boolean isError() {
            return true;
        }

        @Override
        public T getValue() {
            return null;
        }

        @Override
        public Throwable getError() {
            return error;
        }
    }
}
