"""
Metaflow extension registration for metaflow-notifications.
Uses (name, dotted_classpath) tuples for lazy loading to avoid circular imports.
"""

STEP_DECORATORS_DESC = [
    ("notify", ".notify_decorator.NotifyStepDecorator"),
]

FLOW_DECORATORS_DESC = [
    ("notify", ".notify_decorator.NotifyFlowDecorator"),
]
