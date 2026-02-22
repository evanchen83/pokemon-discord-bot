from __future__ import annotations

import io
from typing import Callable

from PIL import Image, ImageDraw

BOX_COLUMNS = 6
CELL_SIZE = 96
SPRITE_SIZE = 80
BOX_PADDING = 12
HEADER_HEIGHT = 28
ROLL_ROWS = 5
ROLL_COLUMNS = 3
ROLL_CELL_SIZE = 112
ROLL_MARKER_WIDTH = 34
POKEBOX_PAGE_SIZE = 24


def theme_colors(theme_type: str | None) -> dict[str, tuple[int, int, int, int]]:
    themes: dict[str, dict[str, tuple[int, int, int, int]]] = {
        "Fire": {
            "bg_top": (255, 235, 220, 255),
            "bg_bottom": (255, 187, 140, 255),
            "panel_fill": (255, 252, 248, 220),
            "header_fill": (255, 196, 157, 255),
            "outline": (186, 92, 50, 255),
            "title_text": (110, 40, 18, 255),
        },
        "Water": {
            "bg_top": (225, 242, 255, 255),
            "bg_bottom": (171, 215, 255, 255),
            "panel_fill": (248, 253, 255, 220),
            "header_fill": (177, 218, 255, 255),
            "outline": (56, 121, 186, 255),
            "title_text": (23, 63, 115, 255),
        },
        "Grass": {
            "bg_top": (232, 250, 232, 255),
            "bg_bottom": (187, 230, 180, 255),
            "panel_fill": (248, 255, 247, 220),
            "header_fill": (191, 231, 185, 255),
            "outline": (67, 137, 61, 255),
            "title_text": (30, 85, 28, 255),
        },
        "Electric": {
            "bg_top": (255, 250, 218, 255),
            "bg_bottom": (255, 233, 143, 255),
            "panel_fill": (255, 255, 245, 220),
            "header_fill": (255, 237, 154, 255),
            "outline": (170, 140, 35, 255),
            "title_text": (93, 78, 19, 255),
        },
    }
    default_theme = {
        "bg_top": (238, 249, 255, 255),
        "bg_bottom": (255, 237, 225, 255),
        "panel_fill": (255, 255, 255, 220),
        "header_fill": (190, 222, 255, 255),
        "outline": (120, 170, 210, 255),
        "title_text": (35, 70, 110, 255),
    }
    if not theme_type:
        return default_theme
    return themes.get(theme_type, default_theme)


def fallback_sprite() -> Image.Image:
    img = Image.new("RGBA", (SPRITE_SIZE, SPRITE_SIZE), (220, 230, 245, 255))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((2, 2, SPRITE_SIZE - 2, SPRITE_SIZE - 2), radius=8, outline=(140, 150, 170, 255), width=2)
    draw.text((SPRITE_SIZE // 2 - 4, SPRITE_SIZE // 2 - 7), "?", fill=(90, 100, 120, 255))
    return img


def build_roll_image(
    rolled: list[list[tuple[int, str]]],
    won_rows: set[int],
    load_resized_sprite: Callable[[int, int], Image.Image],
) -> bytes:
    width = BOX_PADDING * 2 + (ROLL_COLUMNS * ROLL_CELL_SIZE) + ROLL_MARKER_WIDTH
    height = BOX_PADDING * 2 + HEADER_HEIGHT + (ROLL_ROWS * ROLL_CELL_SIZE)

    canvas = Image.new("RGBA", (width, height), (238, 249, 255, 255))
    draw = ImageDraw.Draw(canvas)
    for y in range(height):
        ratio = y / max(1, (height - 1))
        r = int(238 + (255 - 238) * ratio)
        g = int(249 - (12 * ratio))
        b = int(255 - (30 * ratio))
        draw.line((0, y, width, y), fill=(r, g, b, 255))

    draw.rounded_rectangle(
        (BOX_PADDING // 2, BOX_PADDING // 2, width - (BOX_PADDING // 2), height - (BOX_PADDING // 2)),
        radius=14,
        fill=(255, 255, 255, 220),
        outline=(120, 170, 210, 255),
        width=2,
    )
    draw.rounded_rectangle(
        (BOX_PADDING, BOX_PADDING, width - BOX_PADDING, BOX_PADDING + HEADER_HEIGHT),
        radius=10,
        fill=(190, 222, 255, 255),
        outline=(120, 170, 210, 255),
        width=1,
    )
    draw.text((BOX_PADDING + 10, BOX_PADDING + 8), "CATCH ROLL", fill=(35, 70, 110, 255))

    for row_idx, row in enumerate(rolled):
        for col_idx, (pokemon_id, _tier) in enumerate(row):
            cell_x = BOX_PADDING + col_idx * ROLL_CELL_SIZE
            cell_y = BOX_PADDING + HEADER_HEIGHT + row_idx * ROLL_CELL_SIZE
            draw.rounded_rectangle(
                (cell_x + 4, cell_y + 4, cell_x + ROLL_CELL_SIZE - 4, cell_y + ROLL_CELL_SIZE - 4),
                radius=10,
                fill=(245, 250, 255, 255),
                outline=(184, 212, 237, 255),
                width=1,
            )
            sprite = load_resized_sprite(pokemon_id, SPRITE_SIZE)
            paste_x = cell_x + (ROLL_CELL_SIZE - SPRITE_SIZE) // 2
            paste_y = cell_y + (ROLL_CELL_SIZE - SPRITE_SIZE) // 2
            canvas.paste(sprite, (paste_x, paste_y), sprite)

        marker = "OK" if row_idx in won_rows else "X"
        marker_color = (16, 135, 66, 255) if row_idx in won_rows else (178, 34, 34, 255)
        marker_x = BOX_PADDING + (ROLL_COLUMNS * ROLL_CELL_SIZE) + 8
        marker_y = BOX_PADDING + HEADER_HEIGHT + row_idx * ROLL_CELL_SIZE + (ROLL_CELL_SIZE // 2) - 8
        draw.text((marker_x, marker_y), marker, fill=marker_color)

    output = io.BytesIO()
    canvas.save(output, format="PNG", optimize=True, compress_level=9)
    return output.getvalue()


def build_box_image_from_ids(
    pokemon_ids: list[int],
    *,
    box_title: str,
    theme_type: str | None,
    load_resized_sprite: Callable[[int, int], Image.Image],
) -> bytes:
    slots = POKEBOX_PAGE_SIZE
    rows = (slots + BOX_COLUMNS - 1) // BOX_COLUMNS
    width = BOX_PADDING * 2 + BOX_COLUMNS * CELL_SIZE
    height = BOX_PADDING * 2 + HEADER_HEIGHT + rows * CELL_SIZE

    c = theme_colors(theme_type)
    canvas = Image.new("RGBA", (width, height), c["bg_top"])
    draw = ImageDraw.Draw(canvas)
    for y in range(height):
        ratio = y / max(1, (height - 1))
        r = int(c["bg_top"][0] + (c["bg_bottom"][0] - c["bg_top"][0]) * ratio)
        g = int(c["bg_top"][1] + (c["bg_bottom"][1] - c["bg_top"][1]) * ratio)
        b = int(c["bg_top"][2] + (c["bg_bottom"][2] - c["bg_top"][2]) * ratio)
        draw.line((0, y, width, y), fill=(r, g, b, 255))

    draw.rounded_rectangle(
        (BOX_PADDING // 2, BOX_PADDING // 2, width - (BOX_PADDING // 2), height - (BOX_PADDING // 2)),
        radius=14,
        fill=c["panel_fill"],
        outline=c["outline"],
        width=2,
    )
    draw.rounded_rectangle(
        (BOX_PADDING, BOX_PADDING, width - BOX_PADDING, BOX_PADDING + HEADER_HEIGHT),
        radius=10,
        fill=c["header_fill"],
        outline=c["outline"],
        width=1,
    )
    draw.text((BOX_PADDING + 10, BOX_PADDING + 8), box_title.upper(), fill=c["title_text"])

    for idx in range(slots):
        row = idx // BOX_COLUMNS
        col = idx % BOX_COLUMNS
        cell_x = BOX_PADDING + col * CELL_SIZE
        cell_y = BOX_PADDING + HEADER_HEIGHT + row * CELL_SIZE
        draw.rounded_rectangle(
            (cell_x + 3, cell_y + 3, cell_x + CELL_SIZE - 3, cell_y + CELL_SIZE - 3),
            radius=10,
            fill=(245, 250, 255, 235),
            outline=(184, 212, 237, 255),
            width=1,
        )
        if idx < len(pokemon_ids):
            pokemon_id = pokemon_ids[idx]
            sprite = load_resized_sprite(pokemon_id, SPRITE_SIZE)
            paste_x = cell_x + (CELL_SIZE - SPRITE_SIZE) // 2
            paste_y = cell_y + (CELL_SIZE - SPRITE_SIZE) // 2
            canvas.paste(sprite, (paste_x, paste_y), sprite)
        else:
            draw.rounded_rectangle(
                (cell_x + 12, cell_y + 12, cell_x + CELL_SIZE - 12, cell_y + CELL_SIZE - 12),
                radius=8,
                outline=(198, 216, 234, 255),
                width=1,
            )

    output = io.BytesIO()
    canvas.save(output, format="PNG", optimize=True, compress_level=9)
    return output.getvalue()
