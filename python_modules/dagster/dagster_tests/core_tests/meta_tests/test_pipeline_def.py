from dagster import InputDefinition, Nothing, OutputDefinition, pipeline, solid
from dagster.core.meta.dep_snapshot import (
    DependencyStructureSnapshotIndex,
    OutputHandleMeta,
    SolidInvocationMeta,
    build_dep_structure_snapshot_from_pipeline,
)


def test_noop_deps_meta():
    @solid
    def noop_solid(_):
        pass

    @pipeline
    def noop_pipeline():
        noop_solid()

    invocations = build_dep_structure_snapshot_from_pipeline(noop_pipeline).solid_invocation_metas
    assert len(invocations) == 1
    assert isinstance(invocations[0], SolidInvocationMeta)


def test_two_invocations_deps_meta():
    @solid
    def noop_solid(_):
        pass

    @pipeline
    def two_solid_pipeline():
        noop_solid.alias('one')()
        noop_solid.alias('two')()

    index = DependencyStructureSnapshotIndex(
        build_dep_structure_snapshot_from_pipeline(two_solid_pipeline)
    )
    assert index.get_invocation('one')
    assert index.get_invocation('two')


def test_basic_dep():
    @solid
    def return_one(_):
        return 1

    @solid(input_defs=[InputDefinition('value', int)])
    def passthrough(_, value):
        return value

    @pipeline
    def single_dep_pipeline():
        passthrough(return_one())

    index = DependencyStructureSnapshotIndex(
        build_dep_structure_snapshot_from_pipeline(single_dep_pipeline)
    )

    assert index.get_invocation('return_one')
    assert index.get_invocation('passthrough')

    outputs = index.get_upstream_outputs('passthrough', 'value')
    assert len(outputs) == 1
    assert outputs[0].solid_name == 'return_one'
    assert outputs[0].output_name == 'result'


def test_basic_dep_fan_out():
    @solid
    def return_one(_):
        return 1

    @solid(input_defs=[InputDefinition('value', int)])
    def passthrough(_, value):
        return value

    @pipeline
    def single_dep_pipeline():
        return_one_result = return_one()
        passthrough.alias('passone')(return_one_result)
        passthrough.alias('passtwo')(return_one_result)

    index = DependencyStructureSnapshotIndex(
        build_dep_structure_snapshot_from_pipeline(single_dep_pipeline)
    )

    assert index.get_invocation('return_one')
    assert index.get_invocation('passone')
    assert index.get_invocation('passtwo')

    assert index.get_upstream_output('passone', 'value') == OutputHandleMeta('return_one', 'result')
    assert index.get_upstream_output('passtwo', 'value') == OutputHandleMeta('return_one', 'result')


def test_basic_fan_in():
    @solid(output_defs=[OutputDefinition(Nothing)])
    def return_nothing(_):
        return None

    @solid(input_defs=[InputDefinition('nothing', Nothing)])
    def take_nothings(_):
        return None

    @pipeline
    def fan_in_test():
        take_nothings(
            [return_nothing.alias('nothing_one')(), return_nothing.alias('nothing_two')()]
        )

    index = DependencyStructureSnapshotIndex(
        build_dep_structure_snapshot_from_pipeline(fan_in_test)
    )

    assert index.get_invocation('nothing_one')
    assert index.get_invocation('take_nothings')

    assert index.get_upstream_outputs('take_nothings', 'nothing') == [
        OutputHandleMeta('nothing_one', 'result'),
        OutputHandleMeta('nothing_two', 'result'),
    ]
