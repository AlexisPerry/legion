/* Copyright 2017 Stanford University, NVIDIA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef UsecTimer_h
#define UsecTimer_h

#include <time.h>

using namespace std;

class UsecTimer {
public:
    UsecTimer(string description){
        mDescription = description;
        mCumulativeElapsedSeconds = 0.0;
        mNumSamples = 0;
        mStarted = false;
    }
    ~UsecTimer(){}
    void start(){
        if(clock_gettime(CLOCK_MONOTONIC, &mStart)) {
            cerr << "error from clock_gettime" << endl;
            return;
        }
        mStarted = true;
    }
    static double timespecToSeconds(timespec *t) {
        const double nsecToS = 1.0 / 1000000000.0;
        return (double)t->tv_sec + (double)t->tv_nsec * nsecToS;
    }
    void stop(){
        if(mStarted) {
            timespec end;
            if(clock_gettime(CLOCK_MONOTONIC, &end)) {
                cerr << "error from clock_gettime" << endl;
                return;
            }
            double elapsedSeconds = timespecToSeconds(&end) - timespecToSeconds(&mStart);
            mCumulativeElapsedSeconds += elapsedSeconds;
            mNumSamples++;
            mStarted = false;
        }
    }
    string to_string(){
        double meanSampleElapsedSeconds = 0;
        if(mNumSamples > 0) {
            meanSampleElapsedSeconds = mCumulativeElapsedSeconds / mNumSamples;
        }
        double sToUs = 1000000.0;
        std::ostringstream output;
        output << mDescription
        << " " << (mCumulativeElapsedSeconds) << " sec"
        << " " << (mCumulativeElapsedSeconds * sToUs)
        << " usec = " << (meanSampleElapsedSeconds * sToUs)
        << " usec * " << (mNumSamples)
        << (mNumSamples == 1 ? " sample" : " samples");
        return output.str();
    }
    
private:
    bool mStarted;
    struct timespec mStart;
    string mDescription;
    double mCumulativeElapsedSeconds;
    int mNumSamples;
};


#endif /* UsecTimer_h */