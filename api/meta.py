import os
import json

Assets = {}
with open(os.path.join(os.path.dirname(__file__), 'assets.json')) as f:
  Assets = json.load(f)['assets']

ReverseAssets = {value: key for key, value in Assets.items()}

def assetNameToAddress(asset: str) -> str:
  if not asset:
    return None
  asset = asset.strip().lower()
  address = None
  if asset not in Assets:
    if asset in ReverseAssets:
      address = ReverseAssets[asset]
    else:
      address = None
  else:
    address = asset
  return address

def availableAssets() -> list:
  avail = list(Assets.keys())
  avail.extend(ReverseAssets.keys())
  return avail