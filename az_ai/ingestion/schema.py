import json
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    GetJsonSchemaHandler,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    model_serializer,
)
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema


class Fragment(BaseModel):
    """
    A class representing a fragment of document/media.
    """

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the fragment.",
    )
    label: str = Field(..., description="Label for the fragment.")
    metadata: Dict[str, Any] = Field(
        ..., default_factory=dict, description="Metadata associated with the fragment."
    )
    content_ref: str | None = Field(
        default=None,
        description="Reference to the content of the fragment.",
    )

    def human_name(self):
        if self.label:
            return f"{self.label}_{self.id}"
        else:
            return self.id

    @classmethod
    def create_from(cls, fragment, **kwargs: dict[str, Any]) -> "Fragment":
        data = dict(fragment.dict())
        data.pop("class_name", None)
        data.pop("id", None)
        extra_metadata = kwargs.pop("update_metadata", None)
        data.update(kwargs)
        if extra_metadata:
            if "metadata" in data:
                data["metadata"].update(extra_metadata)
            else:
                data["metadata"] = extra_metadata
        return cls(**data)

    # TODO: remove?
    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)

        # inject class name to help with serde
        if "properties" in json_schema:
            json_schema["properties"]["type"] = {
                "title": "Class Name",
                "type": "string",
                "default": cls.class_name(),
            }
        return json_schema

    @classmethod
    def class_name(cls) -> str:
        """
        Get the class name, used as a unique ID in serialization.

        This provides a key that makes serialization robust against actual class
        name changes.
        """
        return "Fragment"

    @model_serializer(mode="wrap")
    def custom_model_dump(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> Dict[str, Any]:
        data = handler(self)
        data["type"] = self.class_name()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs: Any) -> "Fragment":
        data = dict(data)

        assert isinstance(data, dict)
        data_type = data.pop("type")
        if data_type is None:
            raise ValueError("Need to specify type")

        if isinstance(kwargs, dict):
            data.update(kwargs)

        if data_type == Fragment.__name__:
            return Fragment(**data)

        for sub in cls.all_subclasses():
            if data_type == sub.__name__:
                return sub(**data)
        raise TypeError(f"Unsupported sub-type: {data_type}")

    @classmethod
    def from_json(cls, data_str: str, **kwargs: Any) -> "Fragment":  # type: ignore
        data = json.loads(data_str)
        return cls.from_dict(data, **kwargs)

    @classmethod
    def all_subclasses(cls):
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in c.all_subclasses()]
        )

    @classmethod
    def get_subclass(cls, cls_name):
        if cls_name == 'Fragment':
            return Fragment
        for cls in cls.all_subclasses():
            if cls.class_name() == cls_name:
                return cls
        raise ValueError(f"'{cls_name}' is not a subclass of Fragment")


class Document(Fragment):
    content_url: str = Field(
        default_factory=lambda: str(uuid4()),
        description="URL for the content of this document",
    )

    def human_name(self):
        if "file_name" in self.metadata:
            return self.metadata["file_name"]
        else:
            return super().human_name()

    @classmethod
    def class_name(cls) -> str:
        return "Document"

class ImageFragment(Fragment):
    def human_name(self):
        if "file_name" in self.metadata:
            return self.metadata["file_name"]
        else:
            return super().human_name()

    @classmethod
    def class_name(cls) -> str:
        return "ImageFragment"


class Embedding(Fragment):
    vector: List[float] = Field(
        default=None, description="Embedding of the fragment."
    )

    @classmethod
    def class_name(cls) -> str:
        return "Embedding"


class Operation(BaseModel):
    """
    A class representing an operation to be performed on one or more fragments and
    generating one or more new fragments
    """

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the operation.",
    )
    name: str = Field(..., description="Type of the operation.")
    parameters: List[Fragment] = Field(..., description="Parameters for the operation.")
    results: List[Fragment] = Field(
        ..., description="List of fragments generated by the operation."
    )

    def __init__(self, **kwargs):
        """
        Initialize the Operation instance.
        """
        if not kwargs.get("id"):
            kwargs["id"] = str(uuid4())
        super().__init__(**kwargs)


#
# Operations
#

CommandFunctionType = TypeVar("CommandFunctionType", bound=Callable[..., Any])


class FragmentSpec(BaseModel, frozen=True):
    """
    A class representing a fragment specification.
    """

    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    label: str | None = Field(
        default=None,
        description="Label for the output parameter.",
    )

    def matches(self, fragment: Fragment) -> bool:
        """
        Check if the fragment matches the specification.
        """
        if not isinstance(fragment, Fragment.get_subclass(self.fragment_type)):
            return False
        if self.label and fragment.label != self.label:
            return False
        return True

    def __str__(self):
        result = f"{self.fragment_type}"
        if self.label:
            result += f"_{self.label}"
        return result


class OperationInputSpec(BaseModel):
    """
    A class representing the input to an operation function.
    """

    name: str = Field(..., description="Name of the input parameter.")
    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    filter: Dict[str, Any] = Field(..., description="Filter for the input parameter.")

    def specs(self) -> list[FragmentSpec]:
        """
        Convert the OperationInputSpec to a list of FragmentSpec.
        """
        if not self.filter:
            return [FragmentSpec(fragment_type=self.fragment_type)]
        elif isinstance(self.filter, dict):
            label_filters = self.filter.get("label")
            if label_filters is None:
                raise ValueError("Filter must contain a 'label' key.")

            if isinstance(label_filters, str):
                label_filters = [label_filters]
            return [
                FragmentSpec(
                    fragment_type=self.fragment_type,
                    label=label,
                )
                for label in label_filters
            ]
        else:
            raise ValueError("Filter must be a dictionary.")


class OperationOutputSpec(BaseModel):
    """
    A class representing the output of an operation function.
    """

    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    multiple: bool = Field(..., description="Whether the output parameter is multiple.")
    label: str | None = Field(..., description="Label for the output parameter.")

    def spec(self) -> FragmentSpec:
        """
        Convert the OperationOutputSpec to a FragmentSpec.
        """
        return FragmentSpec(
            fragment_type=self.fragment_type,
            label=self.label,
        )

class OperationSpec(BaseModel):
    """
    A class representing an operation function.
    """

    name: str = Field(..., description="Name of the operation function.")
    input: OperationInputSpec
    output: OperationOutputSpec
    func: CommandFunctionType = Field(..., description="The operation function.")

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


#
# Operations Log
#

class OperationsLogEntry(BaseModel):
    """
    A class representing an entry in the operations log.
    """

    operation_name: str = Field(..., description="The performed operation.")
    input_refs: list[str] = Field(..., description="Reference to the input fragments.")
    output_refs: list[str] = Field(..., description="Reference to the output fragments.")

    @classmethod
    def create_from(cls, operation: OperationSpec, input_fragments: list[Fragment], output_fragments: list[Fragment]):
        return OperationsLogEntry(
            operation_name=operation.name,
            input_refs=[fragment.id for fragment in input_fragments],
            output_refs=[fragment.id for fragment in output_fragments],
        )
    

class OperationsLog(BaseModel):
    """
    A class representing the operations log.
    """

    entries: list[OperationsLogEntry] = Field(
        default_factory=list, description="List of operations in the log."
    )

