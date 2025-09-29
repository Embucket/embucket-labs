import os
from datafusion import SessionContext
from datafusion.object_store import AmazonS3

s3 = AmazonS3(
    bucket_name="embucket-testdata",
    region="us-east-2",
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


class DataFusionCursor:
    """Cursor-like wrapper for DataFusion SessionContext to match database cursor interface."""

    def __init__(self, session_context: SessionContext):
        self.ctx = session_context
        self._results = None

    def execute(self, query: str):
        """Execute a SQL query and store the results."""
        self._results = self.ctx.sql(query)
        return self

    def fetchall(self):
        """Fetch all results from the last executed query."""
        if self._results is None:
            return []
        return self._results.collect()

    def fetchone(self):
        """Fetch one result from the last executed query."""
        if self._results is None:
            return None
        results = self._results.collect()
        return results[0] if results else None

    def close(self):
        """Close the cursor (no-op for DataFusion)."""
        pass


def create_datafusion_cursor():
    """Create a DataFusion cursor with a new session context."""
    ctx = SessionContext()
    ctx.register_object_store("s3://", s3, None)
    return DataFusionCursor(ctx)
