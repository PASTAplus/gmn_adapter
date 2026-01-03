# GMN Adapter Notes

20251228-09:03 -- Before creating a call to GMN to synchronize a data package, the adapter should check to see if a predecessor package exists. If so, and if it too has not yet been synchronized with GMN, then it should be synchronized first. This process shoul be performed recursively so that the appropirate obsolescence chain is maintained.

20260102-19:13 --

In Pydantic v2, the standard way to generate a JSON string from a model is by using the **`model_dump_json()`** method.

This method is significantly more powerful than the old `asdict()` approach because it automatically handles the serialization of complex types like **datetimes**, **Enums**, and **nested models**—things that usually cause the standard `json.dumps()` to crash.

---

### 1. Basic Usage

To get a JSON string of your system metadata:

```python
# Create an instance
sys_meta = SysMeta(
    identifier="doi:10.1000/123",
    size=1024,
    checksum="abc123def",
    rights_holder="uid=servilla...",
    date_uploaded="2025-12-31T12:00:00" # String will be coerced to datetime
)

# Dump to JSON string
json_string = sys_meta.model_dump_json()
print(json_string)

```

---

### 2. Common Customizations

You often want to control *how* the JSON is generated, especially for logging or saving to files.

#### Indentation (Pretty Printing)

To make the JSON human-readable (like `json.dumps(indent=4)`):

```python
pretty_json = sys_meta.model_dump_json(indent=4)

```

#### Excluding Unset Values

In your `SysMeta` class, many fields are `Optional`. If you want to keep your JSON file small and only include fields that were actually provided (skipping the `null` values):

```python
compact_json = sys_meta.model_dump_json(exclude_unset=True)

```

#### Using Aliases

If you defined aliases in your model (e.g., `date_uploaded` mapped to `dateUploaded`), you can tell Pydantic to use the **alias** names in the output JSON to match the DataONE XML schema:

```python
xml_compatible_json = sys_meta.model_dump_json(by_alias=True)

```

---

### 3. Comparison of Methods

| Method | Output | Handles Datetimes/Enums? | Best For |
| --- | --- | --- | --- |
| **`model_dump()`** | Python `dict` | No (returns objects) | Internal logic/Database inserts. |
| **`model_dump_json()`** | `str` (JSON) | **Yes** (converts to strings) | API responses / File storage. |
| **`json.dumps(asdict(obj))`** | `str` (JSON) | **No** (Raises TypeError) | Old dataclass pattern. |

---

### 4. Practical Implementation for `gmn_adapter`

Since you are likely logging or caching these records, you can create a helper method or simply use it in your persistence layer:

```python
def save_sysmeta_cache(sysmeta: SysMeta, filepath: str):
    with open(filepath, "w") as f:
        # Write the JSON string directly to a file
        f.write(sysmeta.model_dump_json(indent=2, exclude_none=True))

```