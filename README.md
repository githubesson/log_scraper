# logscraper

Python tool for scraping infostealer logs dropped in telegram channels, with automatic insertion to a mongodb instance.

## prerequisites

Telegram account in the https://t.me/boxedpw channel listed as the 1st message.

Telegram api id and hash - you can get one for free from https://my.telegram.org (i'd recommend an aged account though as they're picky about banning and not banning them), if you get an error when trying to make an app on the site, make sure ur not on a vpn, those are blacklisted.

Telegram premium - if multiple files are dropped at the same time you most likely will get ratelimitted.

MongoDB instance (preferably selfhosted, fk atlas).

Discord webhook (optional, you can remove the code if you dont need it).

## installation

you will need to grab the ripgrep binary from https://github.com/BurntSushi/ripgrep (shoutout), rename it to rg.deb and place it in the script directory on your own, as i've decided to not include any binaries in this repo.

`sudo apt install unrar`

`pip install -r requirements.txt`

after doing these steps, fill out the .env file and you should be good to go.

## contact

https://x.com/ztn

https://misleadi.ng/