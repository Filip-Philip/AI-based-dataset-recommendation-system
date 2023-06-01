# AI-based-dataset-recommendation-system
<!-- > Outline a brief description of your project.
> Live demo [_here_](https://www.example.com). <!-- If you have the project hosted somewhere, include the link here. -->

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Contact](#contact)
<!-- * [License](#license) -->


## General Information
This is a Bachelor's thesis project. We are trying to make research easier by developing a dataset recommendation system.  
The datasets are suggested based on a dataset metadata entry or a text prompt. The data is collected from different repositories across the Web (currently only [Zenodo](https://zenodo.org/), [Harvard Dataverse](https://dataverse.harvard.edu/), [Pennsieve](https://app.pennsieve.io/) are supported but we're soon going to include other repositories as well). The data can be then converted to a common representation, which is then used to create an embedding for each dataset. Having points in space representing datasets we can easily define a notion of similarity. That is our current approach for recommending datasets.
<!-- You don't have to answer all the questions - just the ones relevant to your project. -->
![System Diagram)](https://github.com/Filip-Philip/AI-based-dataset-recommendation-system/assets/92480133/89017cb2-51a1-4c45-8928-e3df18c7193a)


## Technologies Used
The most important libraries used:
- Pandas - version 2.0.1
- Numpy - version 1.24.2
- Requests - version 2.28.2
- Transformers - version 4.29.2
- PyTorch - version 2.0.1


## Features
- Collecting and converting to common representation

<!-- ## Screenshots
![Example screenshot](./img/screenshot.png)
<!-- If you have screenshots you'd like to share, include them here. -->


## Setup
To use the system on your own machine you should:
1) Fork and clone this repository. Type this in the terminal in the directory of your choice.
```
git clone <link_to_your_forked_repository>
```
2) Create a virtual environment.
```
python -m venv venv
```
3) Download all the required packages. (Call this command from the project's root directory)
```
pip install -r requirements.txt
```
4) That's all you need to set up the project! Now it's time to use it!


## Usage
<!-- How does one go about using it?
Provide various use cases and code examples here. -->
1) If you want to download metadata from all the supported repositories use the service file with the ```--all``` option. If not, use the option suiting your needs (i.e. ```--zenodo``` for downloading metadata from just Zenodo)  
2) 🚧 The rest is still in development. Just wait, it's coming soon! 🚧


## Project Status
The project is still in early stages of development! Please be patient, more features coming soon! 🔜


## Room for Improvement
<!-- Include areas you believe need improvement / could be improved. Also add TODOs for future development.

Room for improvement:
- Improvement to be done 1
- Improvement to be done 2 -->

TODO :calendar::
- One function for downloading records "from_date" to "to_date" as .json files for each repository.
- One function for creating a dataframe from all the downloaded .json files and converting it to a merge-able format.
- One function for merging the available dataframes and converting them to the final representation.
- Improve data cleansing.
- Create embeddings from multiple features of dataset metadata.


<!-- ## Acknowledgements
Give credit here.
- This project was inspired by 
- This project was based on [this tutorial](https://www.example.com).
- Many thanks to... -->


## Contact
Created by [@Filip-Philip](https://github.com/Filip-Philip) and [@Qesterius](https://github.com/Qesterius)


<!-- Optional -->
<!-- ## License -->
<!-- This project is open source and available under the [... License](). -->

<!-- You don't have to include all sections - just the one's relevant to your project -->
