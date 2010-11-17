#!/usr/bin/env python

from struct import *;
from collections import namedtuple;
import getopt;
import sys;

class MdbShmReader: 
	def __init__(self,filename):
		self.filename = filename;
		self.readFile();
	def readFile(self):
		try:
			self.fd = open(self.filename,"r");
		except:
			print "open file failed"
			exit(-1);
	def getPoolInfo(self):
		try:
			self.fd.seek(0);
			buf = self.fd.read(20);
			poolinfo = namedtuple('poolinfo','inited page_size total_pgaes free_pages current_page');
			self.mpool = poolinfo._make(unpack('iiiii',buf));
			#print "\033[0;32mMdb Info:\033[0;m"
			print " "
			print " inited		:",self.mpool.inited
			print " page_size	:",self.mpool.page_size
			print " total_pgaes	:",self.mpool.total_pgaes
			print " free_pages	:",self.mpool.free_pages
			print " current_page	:",self.mpool.current_page
			print " "
		except:
			print "getPoolInfo failed"
			pass;
	def getCacheInfo(self):
		try:
			self.fd.seek(524288);
			buf = self.fd.read(16);
			cacheinfo = namedtuple('cacheinfo','inited max_slab_id base_size factor');
			self.cinfo = cacheinfo._make(unpack('iiif',buf));
			#print self.cinfo;
			print " cache info:"
			print " inited		:",self.cinfo.inited;
			print " max_slab_id	:",self.cinfo.max_slab_id;
			print " base_size	:",self.cinfo.base_size;
			print " "
		except:
			pass;

	def getSlabInfo(self):
		#try:
			self.fd.seek(524288+16);
			print "id -- size -- perslab -- item_count--evict_count--full_pages--partial_pages--free_pages"

			for i in range(0,self.cinfo.max_slab_id+1):
				self.fd.read(16); #Pool & Cache pointer
				buf = self.fd.read(16);
				slabinfo = namedtuple('slabinfo','slabid slabsize perslab page_size');
				sinfo = slabinfo._make(unpack('iiii',buf));
				#print sinfo;

				ilist = namedtuple('ilist','head tail');
				buf = self.fd.read(16);
				ilist_e = ilist._make(unpack('LL',buf));
				self.fd.read(32752);

				#self.fd.read(32768); #list & info

				buf = self.fd.read(28);
				
				slabinfo_2 = namedtuple('slabinfo_2','item_count evict_count full_pages partial_pages free_pages');
				sinfo_2 = slabinfo_2._make(unpack('lliii',buf));
				#print sinfo_2;

				self.fd.read(20); #list etc.

				print  "%2d"%sinfo.slabid,"%8d"%sinfo.slabsize,"%8d"%sinfo.perslab,"%12d"%sinfo_2.item_count,"%12d"%sinfo_2.evict_count,"%8d"%sinfo_2.full_pages,"%10d"%sinfo_2.partial_pages,"%10d"%sinfo_2.free_pages,"%10d"%ilist_e.head,"%10d"%ilist_e.tail

				partial_len =  ((sinfo.perslab + 9) / 10 ) * 4;
				self.fd.read(partial_len);

		#except:
		#	pass

	def getHashInfo(self):
		try:
			self.fd.seek(16384);
			buf = self.fd.read(16);
			hashinfo = namedtuple('hashinfo','inited bucket_size item_count start_page');
			self.hinfo = hashinfo._make(unpack('iiii',buf));
			#print self.hinfo;
			print " Hash Info:"
			print " inited		:",self.hinfo.inited;
			print " bucket_size	:",self.hinfo.bucket_size;
			print " item_count	:",self.hinfo.item_count;
			print " start_page	:",self.hinfo.start_page;
			print " "
		except:
			pass;
	def getBitMapInfo(self):
		try:
			self.fd.seek(20);
			buf = self.fd.read(8192);
			start=0;
			end=0;
			prev=();
			for b in buf:
				t = unpack('c',b);
				if prev==():
					pass;
				elif prev == t:
					end += 1;
				else:
					print start,"-",end,":",prev.__str__()[4:6]
					end += 1;
					start = end;
				prev = t;
			print start,"-",end,":",prev.__str__()[4:6];
		except:
			pass;
	def getHashTable(self):
		#try:
			self.getHashInfo();
			self.fd.seek(self.mpool.page_size * self.hinfo.start_page);
			bucket = None;
			for i in range (0,self.hinfo.bucket_size):
				buf = self.fd.read(8);
				bucket = unpack('L',buf);
				if bucket[0] == 0:
					continue;
				print "bucket","%8d"%i,
				old_pos = self.fd.tell();
				while bucket[0] != 0:
					print "->","%20d"%bucket[0],
					page_id,slab_size,page_offset = MdbShmReader.idToDetail(bucket[0]);
					self.fd.seek(page_id * self.mpool.page_size + slab_size * page_offset + 24);
					buf = self.fd.read(8); #h_next
					bucket = unpack('L',buf);
				print ""
				self.fd.seek(old_pos);
		#except:
		#	pass;
			

	def getStatInfo(self):
		try:
			self.fd.seek(32768);
			print "area  quota           datasize     itemcount           hit         get      put            rem         evict"
			for i in range(0,1024):
				buf = self.fd.read(72);
				statinfo = namedtuple('statinfo','quota data_size space_size item_count hit_count get_count put_count remove_count evict_count');
				sinfo = statinfo._make(unpack('LLLLLLLLL',buf));
				if sinfo.quota != 0 or sinfo.item_count != 0:
					print  "%4d"%i,"%12d"%sinfo.quota,"%12d"%sinfo.data_size,"%12d"%sinfo.item_count,"%12d"%sinfo.hit_count,"%12d"%sinfo.get_count,"%12d"%sinfo.put_count,"%12d"%sinfo.remove_count,"%12d"%sinfo.evict_count
		except:
			print "except"
			pass;
	@staticmethod
	def idToDetail(id):
		#try:
		#	self.cinfo
		#except NameError:
		#	self.cinfo = None;
		#if self.cinfo is None:
		#	self.getCacheInfo();
		page_id=((id>>36) & ((1<<16)-1));
		slab_size=((id)&((1<<20)-1));
		page_offset=((id>>20)&((1<<16)-1));
		return (page_id,slab_size,page_offset);

def whichSlab(size):
	start=62
	factor=1.1
	pagesize=1048576
	slab_array=[]
	i=0
	while (i<100 and start < pagesize/2):
		slab_array.append(start);
		start = int(start * factor);
		start = ((start + 7) & (~0x7));	
	slab_array.append(1048552);
	#print slab_array;
	i=0
	while size+46+2 > slab_array[i]:
		i+=1;
	print i,":",slab_array[i]

def main():
	try:
		opts,args = getopt.getopt(sys.argv[1:],"f:i:s:");
	except getopt.GetOptError,err:
		exit(-1);
	viewid=False;
	id=None;
	filename = None;
	slabsize = None;
	for o,a in opts:
		if o == "-i":
			viewid=True;
			try:
				id=int(a,10);
			except ValueError:
				id=int(a,16);
		elif o == "-f":
			filename = a;
		elif o == "-s":
			slabsize = int(a);
	if filename is None and id is None and slabsize is None:
		usage();
		exit(-1);

	if viewid:
		page_id,slab_size,page_offset = MdbShmReader.idToDetail(id);
		print "page_id:",page_id,"slab_size:",slab_size,"page_offset:",page_offset
	elif slabsize:
		whichSlab(slabsize);
	else:
		reader = MdbShmReader(filename);
		reader.getPoolInfo();
		reader.getCacheInfo();
		reader.getHashInfo();
		reader.getStatInfo();
		#reader.getBitMapInfo();
		reader.getSlabInfo();
		#reader.getHashTable();
def usage():
	print "mdbshm_reader.py -f shm file"
	print "		 -i id"
	print "		 -s size"

if __name__ == "__main__":
	main();
