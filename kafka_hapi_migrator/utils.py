import json

def convert_to_json(bundle):
    return json.loads(bundle.decode('utf-8'))


def enrich_bundle(bundle: dict) -> dict:
    """Add FHIR Request metadata."""

    for entry in bundle.get("entry"):
        entry["request"] = {
        "method": "PUT" if entry.get("resource").get("id") else "POST",
        "url": f"{entry['resource']['resourceType']}/{entry['resource']['id']}"
      }

    return bundle

def get_resource_order_index(resource_type: str) -> int:
    """Get the index of the resource type in the resource order list."""
    
    resource_order = ["Organization", "Patient", "Encounter", "DiagnosticReport", "OperationOutcome"]
    
    try:
        return resource_order.index(resource_type)
    except ValueError:
        return len(resource_order)