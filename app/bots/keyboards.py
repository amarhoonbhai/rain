from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def join_gate_kb(channel_username: str, group_invite_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u25A3 Join Channel", url=f"https://t.me/{channel_username.lstrip('@')}")],
        [InlineKeyboardButton(text="\u25A3 Join Group", url=group_invite_url)],
        [InlineKeyboardButton(text="\u25A3 Verify", callback_data="verify")],
    ])


def main_menu_kb(connected: bool = False) -> InlineKeyboardMarkup:
    account_text = "\u2705 Account" if connected else "\u274C Add Account"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=account_text, callback_data="add_account")],
        [InlineKeyboardButton(text="\u25A3 Add Group", callback_data="add_group")],
        [InlineKeyboardButton(text="\u25A3 Delete Group", callback_data="del_gc")],
    ])


def deep_link_login_kb(login_bot_username: str, nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u25A3 Open Login", url=f"https://t.me/{login_bot_username}?start={nonce}")]
    ])


def otp_keypad_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("1", callback_data="d1"), InlineKeyboardButton("2", callback_data="d2"), InlineKeyboardButton("3", callback_data="d3")],
        [InlineKeyboardButton("4", callback_data="d4"), InlineKeyboardButton("5", callback_data="d5"), InlineKeyboardButton("6", callback_data="d6")],
        [InlineKeyboardButton("7", callback_data="d7"), InlineKeyboardButton("8", callback_data="d8"), InlineKeyboardButton("9", callback_data="d9")],
        [InlineKeyboardButton("0", callback_data="d0")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def del_gc_menu_kb(pairs: list[tuple[str, int]]) -> InlineKeyboardMarkup:
    buttons = []
    for title, chat_id in pairs:
        buttons.append([InlineKeyboardButton(text=f"\u25A3 {title}", callback_data=f"del:{chat_id}")])
    buttons.append([InlineKeyboardButton(text="Cancel", callback_data="del:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
