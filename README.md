# ssb-archive

`%k5Br27/PhR10NR9vxS89W5ljY/iMVeIr40OEW006Uvw=.sha256`

- [Patchwork](https://github.com/ssbc/patchwork/)
- [Oasis](https://github.com/fraction/oasis)
- `wget`

```shell
wget --mirror   --page-requisites --no-parent --html-extension --convert-links \
  -e robots=off --reject-regex ".*publish.*|.*public.*|.*mentions.*|.*search.*|.*imageSearch.*|.*settings.*|.*author.*|.*hashtag.*|.*subtopic.*|.*comment.*" \
  --accept-regex ".*profile.*|.*blob.*|.*css.*|.*image.*" \
  http://localhost:3000/profile
```


```shell
wget -N -r -l 1 --no-remove-listing \
  --page-requisites --no-parent --html-extension --convert-links \
  -e robots=off --reject-regex ".*publish.*|.*public.*|.*mentions.*|.*search.*|.*imageSearch.*|.*settings.*|.*author.*|.*hashtag.*|.*subtopic.*|.*comment.*" \
  --accept-regex "/profile/.*|/blob/.*|/css/.*|/image/.*|/thread/.*" \
  http://localhost:3000/profile
```