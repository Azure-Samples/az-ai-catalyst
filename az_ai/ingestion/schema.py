import json
import mimetypes
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    TypeVar,
)
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
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
    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the fragment.",
    )
    label: str = Field(..., description="Label for the fragment.")
    parent_names: List[str] = Field(
        default_factory=list,
        description="List of human-readable parent names for the fragment.",
    )
    human_index: int | None = Field( 
        default=None,
        description="Index of the fragment when multiple fragments with the same label and parent_names are generated.",
    )
    metadata: Dict[str, Any] = Field(
        ..., default_factory=dict, description="Metadata associated with the fragment."
    )
    content_ref: str | None = Field(
        default=None,
        description="Reference to the content of the fragment.",
    )
    mime_type: str = Field(
        default='application/octet-stream',
        description="MIME type of the fragment.",
    )
    content: bytes | None = Field(
        default=None,
        exclude=True,
        description="Binary content associated to this field (not serialized)",
    )

    def human_file_name(self) -> str:
        suffix = mimetypes.guess_extension(self.mime_type)
        index_suffix = f"_{self.human_index:0>3}" if self.human_index is not None else ""
        return "/".join(self.parent_names + [f"{self.label}{index_suffix}{suffix}"])
        
    def __str__(self):
        return f"{self.id}:{self.__class__.__name__}[{self.label}, {self.human_file_name()}]"

    @classmethod
    def create_from(cls, fragment, **kwargs: dict[str, Any]) -> "Fragment":
        data = dict(fragment.dict())
        # Do not copy those 3 fields 
        data.pop("id",  None)
        data.pop("content_ref", None)
        for key in set(data.keys()):
            if key not in cls.model_fields:
                data.pop(key)
        extra_metadata = kwargs.pop("update_metadata", None)
        data.update(kwargs)
        if extra_metadata:
            if "metadata" in data:
                for key, value in dict(extra_metadata).items():
                    if value is None:
                        data["metadata"].pop(key, None)
                        extra_metadata.pop(key, None)
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
    ) -> dict[str, Any]:
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
    content_url: str | None = Field(
        default=None,
        description="URL for the content of this document",
    )

    def human_file_name(self, *args, **kwargs) -> str:
        if "file_name" in self.metadata:
             return self.metadata["file_name"]
        else:
            return super().human_file_name(*args, **kwargs)

    @classmethod
    def class_name(cls) -> str:
        return "Document"

class Chunk(Fragment):
    vector: List[float] = Field(
        default=None, description="Chunk of the fragment."
    )

    @classmethod
    def class_name(cls) -> str:
        return "Chunk"


#
# Operations
#

CommandFunctionType = TypeVar("CommandFunctionType", bound=Callable[..., Any])


class FragmentSelector(BaseModel, frozen=True):
    """
    A class representing a fragment specification.
    """
    model_config = ConfigDict(extra="forbid")

    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    labels: list[str] = Field(
        default=list(),
        description="Labels for the output parameter.",
    )

    def matches(self, fragment: Fragment) -> bool:
        """
        Check if the fragment matches the specification.
        """
        if not isinstance(fragment, Fragment.get_subclass(self.fragment_type)):
            return False
        if len(self.labels) == 0:
            return True
        return fragment.label in self.labels

    def __str__(self):
        result = f"{self.fragment_type}{{"
        if self.labels:
            result += f"_{",".join(self.labels)}}}"
        return result

    def __hash__(self):
        return hash((self.fragment_type, tuple(self.labels)))

class OperationInputSpec(BaseModel):
    """
    A class representing the input to an operation function.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Name of the input parameter.")
    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    multiple: bool = Field(..., description="Whether the input parameter accepts multiple inputs.")
    filter: Dict[str, Any] = Field(..., description="Filter for the input parameter.")

    def selector(self) -> FragmentSelector:
        """
        Get a FragmentSelector for the OperationInputSpec.
        """
        if not self.filter:
            return FragmentSelector(fragment_type=self.fragment_type)
        elif isinstance(self.filter, dict):
            label_filters = self.filter.get("label")
            if label_filters is None:
                raise ValueError("Filter must contain a 'label' key.")

            if isinstance(label_filters, str):
                label_filters = [label_filters]
            return FragmentSelector(
                    fragment_type=self.fragment_type,
                    labels=label_filters,
                )
            
        else:
            raise ValueError("Filter must be a dictionary.")

    def __str__(self):
        result = f"{self.fragment_type}{{"
        if self.filter:
            result += f"{self.filter}"
        return result + "}"


class OperationOutputSpec(BaseModel):
    """
    A class representing the output of an operation function.
    """
    model_config = ConfigDict(extra="forbid")

    fragment_type: str = Field(
        ..., description="Type of the input parameter."
    )
    multiple: bool = Field(..., description="Whether the output parameter is multiple.")
    label: str = Field(..., description="Label for the output parameter.")

    def spec(self) -> FragmentSelector:
        """
        Convert the OperationOutputSpec to a FragmentSelector.
        """
        return FragmentSelector(
            fragment_type=self.fragment_type,
            labels=[self.label],
        )
    
    def __str__(self):
        result = f"{self.fragment_type}{{"
        if self.label:
            result += f"{self.label}"
        return result + f"}}{"*" if self.multiple else ""}"

class OperationSpec(BaseModel):
    """
    A class representing an operation function.
    """
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Name of the operation function.")
    input: OperationInputSpec
    output: OperationOutputSpec
    func: CommandFunctionType = Field(..., description="The operation function.")

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


    def __str__(self):
        return f"{self.name}({self.input}) -> {self.output}"
#
# Operations Log
#

class OperationsLogEntry(BaseModel):
    """
    A class representing an entry in the operations log.
    """
    model_config = ConfigDict(extra="forbid")

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
    model_config = ConfigDict(extra="forbid")

    entries: list[OperationsLogEntry] = Field(
        default_factory=list, description="List of operations in the log."
    )

