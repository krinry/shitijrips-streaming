/**
 * Telegram Session Generator
 * Run this script to generate a session string for Telegram API
 * 
 * Usage: bun generate-session.ts
 */

import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import readline from "readline";

const API_ID = parseInt(process.env.TELEGRAM_API_ID || "26631073");
const API_HASH = process.env.TELEGRAM_API_HASH || "19f3d3108fbadb70e0aa5ecdc33840ed";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

async function main() {
  console.log("🔐 Telegram Session Generator");
  console.log("=============================\n");
  
  const session = new StringSession("");
  const client = new TelegramClient(session, API_ID, API_HASH, {
    deviceModel: "StreamVault",
    appVersion: "1.0.0"
  });

  await client.start({
    phoneNumber: async () => {
      return new Promise((resolve) => {
        rl.question("📱 Enter your phone number (with country code): ", (answer) => {
          resolve(answer);
        });
      });
    },
    password: async () => {
      return new Promise((resolve) => {
        rl.question("🔑 Enter your password: ", (answer) => {
          resolve(answer);
        });
      });
    },
    phoneCode: async () => {
      return new Promise((resolve) => {
        rl.question("📧 Enter the code sent to your phone: ", (answer) => {
          resolve(answer);
        });
      });
    },
    onError: (err) => {
      console.error("❌ Error:", err);
    },
  });

  console.log("\n✅ Successfully logged in!");
  
  const sessionString = client.session.save();
  console.log("\n📝 Your session string is:");
  console.log("---");
  console.log(sessionString);
  console.log("---\n");
  
  console.log("📋 Copy this and add to your .env file as TELEGRAM_SESSION=");
  console.log("\n⚠️  Keep this string secret! It gives access to your Telegram account.");
  
  await client.disconnect();
  rl.close();
}

main().catch(console.error);