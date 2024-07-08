from functools import cache
from typing import List
from pathlib import Path
from dataclasses import fields, astuple
from collections import UserList
from itertools import groupby
import csv

from refscan.lib.Reference import Reference


class ReferenceList(UserList):
    """
    A list of references.

    Note: `UserList` is a base class that facilitates the implementation of custom list classes.
          One thing it does is enable sorting via `sorted(the_list)`.
    """

    def __hash__(self):
        r"""
        Returns an integer that can be used to determine whether one instance is equal to another instance.

        Note: We define this method so that instances of this class are hashable, which allows us to use
              the `@cache` decorator (from `functools`) on methods of this class.

        TODO: Do benchmarking to check whether hashing via `tuple(self.data)` is faster or slower than manually
              hashing each string in each row of `self.data` and hashing the combination of those hashes.

        Reference: https://docs.python.org/3.10/reference/datamodel.html#object.__hash__
        """
        data_as_tuple = tuple(self.data)
        return hash(data_as_tuple)

    @cache
    def get_source_collection_names(self) -> list[str]:
        """
        Returns the distinct `source_collection_names` values among all references in the list.
        """
        distinct_source_collection_names = []
        for reference in self.data:
            if reference.source_collection_name not in distinct_source_collection_names:
                distinct_source_collection_names.append(reference.source_collection_name)
        return distinct_source_collection_names

    @cache
    def get_source_field_names_of_source_collection(self, collection_name: str) -> list[str]:
        """
        Returns the distinct source field names of the specified source collection.
        """
        distinct_source_field_names = []
        for reference in self.data:
            if reference.source_collection_name == collection_name:
                if reference.source_field_name not in distinct_source_field_names:
                    distinct_source_field_names.append(reference.source_field_name)
        return distinct_source_field_names

    @cache
    def get_target_collection_names(
            self,
            source_class_name: str,
            source_field_name: str,
    ) -> list[str]:
        """
        Returns a list of the names of the collections in which a [target] document referenced by the specified field
        of a [source] document representing an instance of the specified schema class, might exist.
        """
        distinct_target_collection_names = []
        references = self.data  # note: in a `UserList`, `self.data` refers to the underlying list data structure
        for reference in references:

            # If this reference's source describes the specified source, record the reference's target collection name.
            if reference.source_field_name == source_field_name and reference.source_class_name == source_class_name:
                if reference.target_collection_name not in distinct_target_collection_names:  # avoids duplicates
                    distinct_target_collection_names.append(reference.target_collection_name)

        return distinct_target_collection_names

    def get_groups(self, field_names: list[str]) -> list[tuple[str, str, str, str, list[str]]]:
        r"""
        Returns an iterable of groups, where each group has a distinct combination of values in the specified fields.

        Note: This method can be used to "consolidate" references that have the same source collection name,
              source field name, and target collection name (i.e. ones that only differ by target class name).
        """

        def make_group_key(reference: Reference) -> tuple:
            """Helper function that returns a key that can be used to group references."""
            values = []
            for field_name in field_names:
                if not hasattr(reference, field_name):
                    raise ValueError(f"No such field: {field_name}")
                values.append(getattr(reference, field_name))
            return tuple(values)

        groups = groupby(sorted(self.data), key=make_group_key)
        return groups

    def dump_to_tsv_file(self, file_path: str | Path) -> None:
        r"""
        Helper function that dumps the references to a TSV file at the specified path.
        """
        column_names = [field_.name for field_ in fields(Reference)]
        with open(file_path, "w", newline="") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")
            writer.writerow(column_names)  # header row
            for reference in self.data:
                writer.writerow(astuple(reference))  # data row

    @cache
    def get_reference_field_names_for_class(self, class_name: str) -> List[str]:
        r"""
        Returns a list of the names of this class's fields that can contain references.
        """
        names_of_reference_fields: List[str] = []
        for reference in self.data:
            if reference.source_class_name == class_name:
                if reference.source_field_name not in names_of_reference_fields:
                    names_of_reference_fields.append(reference.source_field_name)

        return names_of_reference_fields
