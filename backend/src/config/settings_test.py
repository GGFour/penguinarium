from . import settings as base  # type: ignore

# Re-export everything from base
globals().update({k: getattr(base, k) for k in dir(base) if not k.startswith("_")})

# Use SQLite in-memory database for tests to avoid requiring Postgres
DATABASES = globals().get("DATABASES", {})
DATABASES["default"] = {
    "ENGINE": "django.db.backends.sqlite3",
    "NAME": ":memory:",
}

# Ensure migrations run quickly in SQLite
PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.MD5PasswordHasher",
]
