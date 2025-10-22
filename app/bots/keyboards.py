from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

def join_gate_kb(channel_username: str, group_invite_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▣ Join Channel", url=f"https://t.me/{channel_username.lstrip('@')}")],
        [InlineKeyboardButton(text="▣ Join Group", url=group_invite_url)],
        [InlineKeyboardButton(text="▣ Verify", callback_data="verify")],
    ])

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▣ Add Account", callback_data="add_account")],
        # (other items will arrive in later steps)
    ])

def deep_link_login_kb(login_bot_username: str, nonce: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▣ Open Login", url=f"https://t.me/{login_bot_username}?start={nonce}")]
    ])
