# Pumptrend Bot - Solana Memecoin Telegram Announcer

A Python Telegram bot that automatically discovers trending memecoin tokens on Pump.fun and allows users to promote their tokens for a fee.

## Features

✨ **Automatic Token Discovery**
- Monitors new tokens on Pump.fun every 30 seconds
- Filters by market cap range ($7,500 - $20,000)
- Filters by token age (≤ 1 hour from creation)
- Posts formatted messages to Telegram channel

💰 **Promotion System**
- Users can promote their own tokens for 0.5 SOL
- Payment verification via blockchain
- Custom social links (Telegram, Twitter)
- Beautiful formatted messages with links

🔗 **Integration**
- Helius RPC for Solana transactions
- Birdeye API for token data (market cap, holders, logo)
- Dexscreener for trading links
- Pump.fun for token discovery

## Setup

### 1. Clone Repository
```bash
git clone <your-repo>
cd pumptrend-bot
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
cp .env.example .env
```

Required values:
- `BOT_TOKEN`: Get from @BotFather on Telegram
- `CHANNEL_ID`: Your channel ID (format: -100123456789 or @channelname)
- `HELIUS_API_KEY`: From https://www.helius.dev
- `BIRDEYE_API_KEY`: From https://public-api.birdeye.so
- `PAYMENT_WALLET`: Your Solana wallet address

### 5. Run Bot

```bash
python main.py
```

## Architecture

```
pumptrend-bot/
├── main.py                  # Main bot application
├── config.py               # Configuration management
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (secret)
├── .gitignore             # Git ignore rules
├── README.md              # This file
│
├── handlers/              # Message & callback handlers
│   └── user_handlers.py   # User interaction handlers
│
├── tasks/                 # Background tasks
│   └── token_monitor.py   # Token discovery & posting
│
├── utils/                 # Utility modules
│   ├── solana_utils.py    # Solana RPC interactions
│   ├── birdeye_utils.py   # Birdeye API client
│   ├── dexscreener_utils.py # Dexscreener API client
│   └── pump_utils.py      # Pump.fun API client
│
└── formatters/            # Message formatters
    └── message_formatter.py # Token message formatting
```

## Workflow

### Automatic Discovery
1. Bot polls Pump.fun API every 30 seconds
2. Filters new tokens by age and market cap
3. Fetches token data from Birdeye (market cap, holders, logo)
4. Posts formatted message to Telegram channel
5. Includes "Promote This Token" button

### Promotion by Users
1. User clicks "Promote This Token" button
2. Bot asks for token contract address (CA)
3. Bot confirms token details (name, market cap, holders)
4. User provides social links (Telegram, Twitter)
5. Bot requests payment: 0.5 SOL to payment wallet
6. User sends SOL and provides transaction hash
7. Bot verifies transaction on blockchain
8. Bot posts promotional message to channel
9. User receives confirmation

## API Rate Limits

- **Birdeye**: Up to 100 requests/minute (standard tier)
- **Dexscreener**: Up to 300 requests/minute
- **Helius**: Up to 1M credits/month (each call = 10 credits)
- **Telegram**: Standard rate limits apply

## Logging

Logs are written to:
- `bot.log` file
- Console output

Check logs for debugging:
```bash
tail -f bot.log
```

## Development Notes

### Testing Locally
1. Use polling instead of webhooks (no public IP needed)
2. Adjust `POLLING_INTERVAL` in `.env` (default: 30 seconds)
3. Use testnet tokens for initial testing

### Production Deployment

For VPS deployment:
```bash
# Install systemd service
sudo cp pumptrend-bot.service /etc/systemd/system/

# Enable and start
sudo systemctl enable pumptrend-bot
sudo systemctl start pumptrend-bot
```

## Troubleshooting

### Bot not posting tokens
- Check Birdeye API key is valid
- Verify token market cap filters
- Check logs: `tail -f bot.log`

### Payment verification failing
- Verify Helius API key
- Check payment wallet address
- Ensure transaction is confirmed

### Telegram errors
- Verify BOT_TOKEN is correct
- Ensure CHANNEL_ID is valid
- Check bot has admin permissions in channel

## Support

For issues, check the logs and verify:
1. All API keys are valid
2. Bot has proper permissions
3. Environment variables are set correctly

## License

MIT License - See LICENSE file

## Disclaimer

This bot is for informational purposes only. Always do your own research (DYOR) on tokens. The bot creators are not responsible for losses or scams.