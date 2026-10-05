import {
  GraphQLError,
  ValidationContext,
  ASTVisitor,
  OperationDefinitionNode,
  FieldNode,
  InlineFragmentNode,
  FragmentSpreadNode,
  SelectionSetNode,
  Kind,
} from "graphql";

export function depthLimit(maxDepth = 6) {
  return (context: ValidationContext): ASTVisitor => {
    return {
      OperationDefinition(node: OperationDefinitionNode) {
        const depth = getDepth(node.selectionSet, context, 1);
        if (depth > maxDepth) {
          context.reportError(
            new GraphQLError(
              `Query depth of ${depth} exceeds maximum allowed depth of ${maxDepth}.`,
              { nodes: [node] }
            )
          );
        }
      },
    };
  };
}

function getDepth(
  selectionSet: SelectionSetNode | undefined,
  context: ValidationContext,
  currentDepth: number
): number {
  if (!selectionSet || !selectionSet.selections || selectionSet.selections.length === 0) {
    return currentDepth;
  }

  let maxChildDepth = currentDepth;

  for (const selection of selectionSet.selections) {
    if (selection.kind === Kind.FIELD) {
      const field = selection as FieldNode;
      // Skip introspection fields like __schema, __type
      if (field.name.value.startsWith("__")) {
        continue;
      }
      if (field.selectionSet) {
        const childDepth = getDepth(field.selectionSet, context, currentDepth + 1);
        if (childDepth > maxChildDepth) {
          maxChildDepth = childDepth;
        }
      }
    } else if (selection.kind === Kind.INLINE_FRAGMENT) {
      const fragment = selection as InlineFragmentNode;
      if (fragment.selectionSet) {
        const childDepth = getDepth(fragment.selectionSet, context, currentDepth);
        if (childDepth > maxChildDepth) {
          maxChildDepth = childDepth;
        }
      }
    } else if (selection.kind === Kind.FRAGMENT_SPREAD) {
      const spread = selection as FragmentSpreadNode;
      const fragment = context.getFragment(spread.name.value);
      if (fragment && fragment.selectionSet) {
        const childDepth = getDepth(fragment.selectionSet, context, currentDepth);
        if (childDepth > maxChildDepth) {
          maxChildDepth = childDepth;
        }
      }
    }
  }

  return maxChildDepth;
}
