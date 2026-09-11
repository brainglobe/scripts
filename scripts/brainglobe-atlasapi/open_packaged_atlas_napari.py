# /// script
# dependencies = [
#     "napari[all]",
#     "brainglobe-atlasapi",
#     "brainrender-napari",
# ]
# ///

import napari
from brainglobe_atlasapi import BrainGlobeAtlas
from brainrender_napari.napari_atlas_representation import (
    NapariAtlasRepresentation,
)
from brainrender_napari.widgets.structure_view import (
    StructureTreeModel,
    StructureView,
)

atlas = BrainGlobeAtlas(
    "nmt_arm_sym_macaque_250um",
    brainglobe_dir="/home/harrycarey/brainglobe_workingdir/nmt_arm_sym_macaque",
    check_latest=False,
)

viewer = napari.Viewer(ndisplay=3)

# Display reference, annotations, additional references, and the root mesh.
display = NapariAtlasRepresentation(atlas, viewer)
display.add_to_viewer()

for reference_name in atlas.additional_references:
    display.add_additional_reference(reference_name)

display.add_structure_to_viewer("root")

# Add the hierarchy using the atlas already loaded from your working directory.
tree = StructureView()
tree_model = StructureTreeModel(atlas.structures_list)
tree.setModel(tree_model)
tree.hideColumn(2)  # Hide numeric IDs; show acronyms and names.
tree.setHeaderHidden(True)
tree.setExpandsOnDoubleClick(False)
tree.expandToDepth(1)
tree.resizeColumnToContents(0)

# Double-click a region to add its mesh.
tree.add_structure_requested.connect(display.add_structure_to_viewer)
viewer.window.add_dock_widget(
    tree,
    name="Atlas hierarchy",
    area="right",
)

napari.run()
