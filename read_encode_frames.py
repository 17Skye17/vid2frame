import os
import sys
import numpy as np
import argparse
import lmdb
import json

import subprocess
from cStringIO import StringIO
from tqdm import tqdm
from subprocess import call
import cPickle as pickle

parser = argparse.ArgumentParser()
parser.add_argument("split_file", type=str, help="the split file")
parser.add_argument("split", type=str, help="the split to use")
parser.add_argument("frame_db", type=str, help="the directory to store extracted frames, an LMDB")
parser.add_argument("-a", "--asis", action="store_true", help="do not resize frames, s,w,h are all ignored in this case")
parser.add_argument("-s", "--short", type=int, default=0, help="scale the shorter side, w and h are ignored in this case")
parser.add_argument("-H", "--height", type=int, default=0, help="the resize height")
parser.add_argument("-W", "--width", type=int, default=0, help="the resize width")
parser.add_argument("-k", "--skip", type=int, default=1, help="only store frames with (ID-1) mod skip==0, ID starts from 1")
parser.add_argument("-n", "--num_frame", type=int, default=-1, help="uniformly sample n frames, this will override --skip")
parser.add_argument("-r","--interval", type=int, default=0, help="extract images from video every r frames")
args = parser.parse_args()


split = pickle.load(open(args.split_file,'rb'))
print split.keys(), 'using %s' %(args.split)
all_videos = split[args.split]

frame_db = lmdb.open(args.frame_db, map_size=1<<40)

ram_dir = './cache'
    
def read_img(path):
    with open(path, 'rb') as f:
        return f.read()

num_frames = {}


def get_frame_rate(vid):
    call = ["ffprobe","-v","quiet","-show_entries","stream=r_frame_rate","-print_format","json",vid]
    output = subprocess.check_output(call)
    output = json.loads(output)
    r_frame_rate = 0
    if len(output.keys()) == 0:
        return r_frame_rate
    elif output['streams'] == []:
        return r_frame_rate

    for line in output['streams']:
        nums = line['r_frame_rate'].split('/')
        if float(nums[1]) == 0:
            continue
        frame_rate = 1.0*float(nums[0]) / float(nums[1])
        if frame_rate != 0:
            r_frame_rate = frame_rate
    return r_frame_rate

for vid in tqdm(all_videos, ncols=64):
    vvid = vid.split('/')[-1].split('.')[0]
    num_frames[vvid] = []

    v_dir = os.path.join(ram_dir, vvid)
    call(["rm", "-rf", v_dir])
    os.mkdir(v_dir)    # caching directory to store ffmpeg extracted frames


    if args.asis:
        call(["ffmpeg", "-loglevel", "panic", "-i", vid, "-qscale:v", "2", v_dir+"/%6d.jpg"])

    elif args.short > 0:
        call(["ffmpeg",
                "-loglevel", "panic",
                "-i", vid,
                "-vf", "scale='iw*1.0/min(iw,ih)*%d':'ih*1.0/min(iw,ih)*%d'" % (args.short, args.short),
                "-qscale:v", "2",
                v_dir+"/%6d.jpg"])
    elif args.interval > 0:
        basename = os.path.basename(vid).split('.')[0]
	r_frame_rate = get_frame_rate(vid)
	if r_frame_rate == 0:
	    print "frame rate is 0, skip: %s"%vid
	    continue
	call(["ffmpeg",
		"-loglevel","panic",
		"-i",vid,
		"-vf","scale=%d:%d" % (args.width,args.height),
		"-vf","select=not(mod(n\,%d*%f))" % (args.interval,r_frame_rate),
                "-vsync","vfr",
		"-qscale:v","2",
		v_dir+"/%6d.jpg"])
    else:
        call(["ffmpeg",
                "-loglevel", "panic",
                "-i", vid,
                "-vf", "scale=%d:%d" % (args.width, args.height),
                "-qscale:v", "2",
                v_dir+"/%6d.jpg"])

    sample = (args.num_frame > 0)
    with frame_db.begin(write=True, buffers=True) as txn:
        if sample:
            ids = [int(f.split('.')[0]) for f in os.listdir(v_dir)]
            sample_ids = set(list(np.linspace(min(ids), max(ids),
                                    args.num_frame, endpoint=True, dtype=np.int32)))

        for f_path in os.listdir(v_dir):
            fid = int(f_path.split('.')[0])

            if sample:
                if fid not in sample_ids:
                    continue
            elif args.skip > 1:
                if (fid-1) % args.skip != 0:
                    continue

            num_frames[vvid].append(fid)

            s = read_img(os.path.join(v_dir, f_path))

            key = "%s/%08d" % (vvid, fid)   # by padding zeros, frames in LMDB are stored in order
            txn.put(key, s)

    call(["rm", "-rf", v_dir])


with frame_db.begin(write=True, buffers=True) as txn:
    txn.put('num_frames', pickle.dumps(num_frames))

