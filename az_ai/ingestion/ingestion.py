import logging
import inspect
from typing import Callable, TypeVar, Any, get_args, get_type_hints

from rich.console import Console

logger = logging.getLogger(__name__)

CommandFunctionType = TypeVar("CommandFunctionType", bound=Callable[..., Any])

class TransformationError(Exception):
    pass

class TransformationInfo:
    """
    A class representing a transformation function.
    """
    def __init__(self, func: CommandFunctionType):
        self.func = func

        self.parse_signature(func)

    def name(self) -> str:
        """
        Get the name of the transformation function
        """
        return self.func.__name__

    def parse_signature(self, func: CommandFunctionType):
        """
        Parse the signature of the function to extract its parameters and return type
        """
        logger.debug("Parsing function signature for %s...", func.__name__)

        type_hints = get_type_hints(func)
        signature = inspect.signature(func)
        if len(signature.parameters) == 0:
            raise TransformationError("Transformation function must have parameters.")
        
        if signature.return_annotation is inspect.Signature.empty:
            raise TransformationError("Transformation function must have a return type.")
        

        for param in signature.parameters.values():
            param_name = param.name
            param_type = type_hints.get(param_name, Any)
            logger.debug("Parameter: %s, Type: %s", param_name, param_type)

            # Check if the parameter type is a generic typ

            # If you want to handle specific types or do something with them,
            # you can add your logic here.
            # For example, if you want to check if the parameter is a Fragment:
            # if param_type == Fragment:
            #     logger.debug("This parameter is a Fragment.")
    


        # You can use inspect.signature(func) to get the actual signature
        # and extract parameter names and types if needed.
    

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

class Ingestion:
    def __init__(self):
        self.transformations = []

    def transformation(self) -> Callable[[CommandFunctionType], CommandFunctionType]:
        def decorator(func: CommandFunctionType) -> CommandFunctionType:
            logger.debug("Registering transformation function %s...", func.__name__)
            self.transformations.append(TransformationInfo(func))
            return func

        return decorator
    
    def __call__(self, *args, **kwargs):
        console = Console()
        console.print(f"Run ingestion pipeline with args: {kwargs}", )
        for transformation in self.transformations:
            console.print(f"  Run [bold]{transformation.name()}[/]...", )
            transformation(1)