"""End-to-end Netherlands address -> real PDOK Solar scene pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

import trimesh

from solar.adapters.bag_address import (
    BagAddressResolution,
    resolve_bag_address,
)
from solar.adapters.netherlands import (
    PdokBuildingTile,
    discover_building_tiles,
)
from solar.adapters.netherlands_scene import (
    NetherlandsLocalScene,
    load_cached_pdok_scene,
)


DEFAULT_CACHE_DIR = (
    Path.home()
    / ".domiframe-solar-cache"
    / "pdok"
)


@dataclass
class NetherlandsAddressScene:
    address: BagAddressResolution
    target_id: str
    radius_m: float
    tiles: tuple[PdokBuildingTile, ...]
    archive_paths: tuple[Path, ...]
    tile_scenes: tuple[NetherlandsLocalScene, ...]
    meshes: dict[str, trimesh.Trimesh]
    target_tile_scene: NetherlandsLocalScene
    duplicate_building_ids: tuple[str, ...]

    @property
    def lat(self) -> float:
        if self.address.lat is None:
            raise ValueError(
                "Resolved BAG address has no latitude."
            )

        return self.address.lat

    @property
    def lon(self) -> float:
        if self.address.lon is None:
            raise ValueError(
                "Resolved BAG address has no longitude."
            )

        return self.address.lon

    @property
    def x(self) -> float:
        return self.target_tile_scene.x

    @property
    def y(self) -> float:
        return self.target_tile_scene.y

    @property
    def target_mesh(self) -> trimesh.Trimesh:
        try:
            return self.meshes[
                self.target_id
            ]
        except KeyError as exc:
            raise LookupError(
                f"Target mesh missing: {self.target_id}"
            ) from exc


def latest_tiles_per_sheet(
    tiles: Iterable[PdokBuildingTile],
) -> list[PdokBuildingTile]:
    """Keep the newest available tile independently for every sheet."""

    grouped: dict[
        str,
        list[PdokBuildingTile],
    ] = {}

    for tile in tiles:
        grouped.setdefault(
            tile.sheet_id,
            [],
        ).append(
            tile
        )

    selected = []

    for sheet_id, candidates in grouped.items():
        candidates.sort(
            key=lambda item: (
                int(
                    item.imagery_year
                    or -1
                ),
                item.start_date
                or "",
                item.end_date
                or "",
            ),
            reverse=True,
        )

        selected.append(
            candidates[0]
        )

    selected.sort(
        key=lambda item:
        item.sheet_id
    )

    return selected


def tile_cache_filename(
    tile: PdokBuildingTile,
) -> str:
    """Return a deterministic cache filename from the official URL."""

    path = urlparse(
        tile.download_url
    ).path

    name = Path(
        path
    ).name

    if not name:
        raise ValueError(
            "PDOK tile URL has no filename: "
            f"{tile.download_url}"
        )

    if not name.lower().endswith(
        ".zip"
    ):
        raise ValueError(
            "PDOK building tile is not a ZIP archive: "
            f"{tile.download_url}"
        )

    return name


def valid_zip_archive(
    path: Path,
) -> bool:
    """Check that a cached PDOK ZIP can be opened and has no corrupt members."""

    if (
        not path.exists()
        or not path.is_file()
        or path.stat().st_size == 0
    ):
        return False

    try:
        with ZipFile(
            path
        ) as archive:
            members = archive.namelist()

            if not members:
                return False

            if not any(
                name.lower().endswith(
                    ".city.json"
                )
                for name in members
            ):
                return False

            return (
                archive.testzip()
                is None
            )

    except (
        BadZipFile,
        OSError,
    ):
        return False


def cache_pdok_tile(
    tile: PdokBuildingTile,
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    timeout_s: float = 120.0,
) -> Path:
    """Return a verified local copy of one official PDOK 3D tile."""

    cache_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination = (
        cache_dir
        / tile_cache_filename(
            tile
        )
    )

    if valid_zip_archive(
        destination
    ):
        return destination

    temporary = destination.with_suffix(
        destination.suffix
        + ".part"
    )

    if temporary.exists():
        temporary.unlink()

    request = Request(
        tile.download_url,
        headers={
            "User-Agent": (
                "DomiFrame-Solar/1.0"
            ),
        },
    )

    try:
        with urlopen(
            request,
            timeout=timeout_s,
        ) as response, temporary.open(
            "wb"
        ) as target:
            shutil.copyfileobj(
                response,
                target,
                length=1024 * 1024,
            )

        if not valid_zip_archive(
            temporary
        ):
            raise RuntimeError(
                "Downloaded PDOK archive failed ZIP validation: "
                f"{tile.download_url}"
            )

        temporary.replace(
            destination
        )

    except Exception:
        if temporary.exists():
            temporary.unlink()

        raise

    return destination


def _combine_tile_scenes(
    tile_scenes: Iterable[NetherlandsLocalScene],
) -> tuple[
    dict[str, trimesh.Trimesh],
    tuple[str, ...],
]:
    """Combine tile meshes while safely handling possible edge duplicates."""

    combined: dict[
        str,
        trimesh.Trimesh,
    ] = {}

    duplicates = set()

    for scene in tile_scenes:
        for building_id, mesh in scene.meshes.items():
            existing = combined.get(
                building_id
            )

            if existing is None:
                combined[
                    building_id
                ] = mesh
                continue

            duplicates.add(
                building_id
            )

            # Tile boundaries can theoretically repeat a building.
            # Keep the representation containing more geometric detail.
            if len(
                mesh.faces
            ) > len(
                existing.faces
            ):
                combined[
                    building_id
                ] = mesh

    return (
        combined,
        tuple(
            sorted(
                duplicates
            )
        ),
    )


def load_netherlands_address_scene(
    *,
    postcode: str,
    house_number: int,
    house_letter: str | None = None,
    addition: str | None = None,
    radius_m: float = 75.0,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    discovery_timeout_s: float = 30.0,
    download_timeout_s: float = 120.0,
) -> NetherlandsAddressScene:
    """Resolve an address and build its complete local PDOK 3D scene."""

    address = resolve_bag_address(
        postcode=postcode,
        house_number=house_number,
        house_letter=house_letter,
        addition=addition,
    )

    if (
        address.lat is None
        or address.lon is None
    ):
        raise LookupError(
            "BAG address resolved without coordinates."
        )

    if not address.panden:
        raise LookupError(
            "BAG address has no related Pand."
        )

    if len(
        address.panden
    ) != 1:
        raise LookupError(
            "Address relates to multiple BAG Panden. "
            "Multi-building address handling is not implemented yet. "
            f"Count={len(address.panden)}"
        )

    target_id = (
        address.panden[
            0
        ].cityjson_object_id
    )

    discovered = discover_building_tiles(
        lat=address.lat,
        lon=address.lon,
        radius_m=radius_m,
        timeout_s=discovery_timeout_s,
    )

    tiles = latest_tiles_per_sheet(
        discovered
    )

    if not tiles:
        raise LookupError(
            "PDOK 3D discovery returned no building tiles "
            f"within {radius_m:.1f} m of the address."
        )

    archives = []

    tile_scenes = []

    target_scenes = []

    for tile in tiles:
        archive_path = cache_pdok_tile(
            tile,
            cache_dir=cache_dir,
            timeout_s=download_timeout_s,
        )

        archives.append(
            archive_path
        )

        scene = load_cached_pdok_scene(
            archive_path=archive_path,
            lat=address.lat,
            lon=address.lon,
            radius_m=radius_m,
        )

        tile_scenes.append(
            scene
        )

        if target_id in scene.meshes:
            target_scenes.append(
                scene
            )

    if not target_scenes:
        available_count = sum(
            len(
                scene.meshes
            )
            for scene in tile_scenes
        )

        raise LookupError(
            "Exact BAG Pand was not found in the discovered PDOK "
            "LoD scene. "
            f"Target={target_id}; "
            f"tiles={len(tiles)}; "
            f"loaded_meshes={available_count}"
        )

    if len(
        target_scenes
    ) > 1:
        # A building on a tile boundary may occur in more than one tile.
        # Prefer the representation with the most detailed mesh.
        target_scenes.sort(
            key=lambda scene:
            len(
                scene.meshes[
                    target_id
                ].faces
            ),
            reverse=True,
        )

    target_tile_scene = (
        target_scenes[
            0
        ]
    )

    meshes, duplicates = (
        _combine_tile_scenes(
            tile_scenes
        )
    )

    if target_id not in meshes:
        raise LookupError(
            "Exact target disappeared while combining tile scenes: "
            f"{target_id}"
        )

    return NetherlandsAddressScene(
        address=address,
        target_id=target_id,
        radius_m=float(
            radius_m
        ),
        tiles=tuple(
            tiles
        ),
        archive_paths=tuple(
            archives
        ),
        tile_scenes=tuple(
            tile_scenes
        ),
        meshes=meshes,
        target_tile_scene=target_tile_scene,
        duplicate_building_ids=duplicates,
    )
