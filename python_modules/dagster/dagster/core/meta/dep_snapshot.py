from collections import namedtuple

from dagster import check


def _output_handles_to_metas(output_handles):
    return list(map(lambda oh: OutputHandleMeta(oh.solid.name, oh.output_def.name), output_handles))


def build_dep_structure_snapshot_from_pipeline(pipeline_def):
    invocations = []
    dep_structure = pipeline_def.dependency_structure
    for solid in pipeline_def.solids:
        input_dep_metas = []
        for input_handle, output_handles in dep_structure.input_to_upstream_outputs_for_solid(
            solid.name
        ).items():
            input_dep_metas.append(
                InputDependencyMeta(
                    input_name=input_handle.input_def.name,
                    prev_output_metas=_output_handles_to_metas(output_handles),
                )
            )

        invocations.append(
            SolidInvocationMeta(
                name=solid.name,
                solid_def_name=solid.definition.name,
                tags=solid.tags,
                input_dep_metas=input_dep_metas,
            )
        )

    return DependencyStructureSnapshot(invocations)


class DependencyStructureSnapshot(
    namedtuple('_DependencyStructureSnapshot', 'solid_invocation_metas')
):
    pass


class DependencyStructureSnapshotIndex:
    def __init__(self, dep_structure_snapshot):
        check.inst_param(
            dep_structure_snapshot, 'dep_structure_snapshot', DependencyStructureSnapshot
        )
        self._invocations_dict = {
            si.name: si for si in dep_structure_snapshot.solid_invocation_metas
        }

    def get_invocation(self, solid_name):
        check.str_param(solid_name, 'solid_name')
        return self._invocations_dict[solid_name]

    def get_upstream_outputs(self, solid_name, input_name):
        check.str_param(solid_name, 'solid_name')
        check.str_param(input_name, 'input_name')

        for input_dep in self.get_invocation(solid_name).input_dep_metas:
            if input_dep.input_name == input_name:
                return input_dep.prev_output_metas

        check.failed(
            'Input {input_name} not found for solid {solid_name}'.format(
                input_name=input_name, solid_name=solid_name,
            )
        )

    def get_upstream_output(self, solid_name, input_name):
        outputs = self.get_upstream_outputs(solid_name, input_name)
        check.invariant(len(outputs) == 1)
        return outputs[0]


class OutputHandleMeta(namedtuple('_OutputHandleMeta', 'solid_name output_name')):
    pass


class InputDependencyMeta(namedtuple('_InputDependencyMeta', 'input_name prev_output_metas')):
    pass


class SolidInvocationMeta(
    namedtuple('_SolidInvocationMeta', 'name solid_def_name tags input_dep_metas')
):
    pass
