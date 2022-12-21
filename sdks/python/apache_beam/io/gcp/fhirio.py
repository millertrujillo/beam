# encoding: utf-8
"""Fhir IO wrapper."""

from typing import Optional, NamedTuple

from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

ImportConfig = NamedTuple("ImportConfig", [
    ("fhir_store", str),
    ("dead_letter_gcs_path", str),
    ("temp_gcs_path", Optional[str]),
    ("content_structure", Optional[str]),
])


class Import(ExternalTransform):
    """
        An external PTransform which import to Healthcare API Fhir Store.

        Experimental; no backwards compatibility guarantees.
    """
    URN = "beam:transform:org.apache.beam:fhir_import:v1"

    def __init__(self,
                 fhir_store,
                 dead_letter_gcs_path,
                 temp_gcs_path=None,
                 content_structure=None,
                 expansion_service=None):
        super().__init__(
            self.URN,
            NamedTupleBasedPayloadBuilder(
                ImportConfig(
                    fhir_store=fhir_store,
                    dead_letter_gcs_path=dead_letter_gcs_path,
                    temp_gcs_path=temp_gcs_path,
                    content_structure=content_structure,
                )), expansion_service)
